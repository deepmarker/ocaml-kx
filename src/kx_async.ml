open Core
open Async
open Kx

let src = Logs.Src.create "kx.async"
module Log = (val Logs.src_log src : Logs.LOG)
module Log_async = (val Logs_async.src_log src : Logs_async.LOG)

type msg = K : { big_endian:bool; typ:'a w;  msg:'a } -> msg
let create ?(big_endian=Sys.big_endian) typ msg = K { big_endian; typ; msg }

let parse_compressed ~big_endian ~msglen w r =
  let module E = (val (getmod big_endian) : ENDIAN) in
  let open Deferred.Result.Monad_infix in
  Angstrom_async.parse E.any_int32 r >>= fun uncompLen ->
  let uncompLen = Int32.to_int_exn uncompLen in
  Angstrom_async.parse (Angstrom.take_bigstring (msglen-4)) r >>= fun compMsg ->
  let uncompMsg = Bigstringaf.create uncompLen in
  uncompress uncompMsg compMsg ;
  let res =
    Angstrom.parse_bigstring
      (destruct ~big_endian w)
      (Bigstringaf.sub uncompMsg ~off:8 ~len:(uncompLen-8)) in
  return res

let write_handshake w url =
  Writer.write w
    (Printf.sprintf "%s:%s\x03\x00"
       (Option.value ~default:"" (Uri.user url))
       (Option.value ~default:"" (Uri.password url)))

module Async = struct
  type t =  {
    r: 'a. 'a w -> 'a Deferred.Or_error.t ;
    w: msg Pipe.Writer.t ;
  }

  module T = struct
    module Address = struct
      include Uri_sexp
      let equal a b = compare a b = 0
    end

    type nonrec t = t

    let close { w; _ } = Pipe.close w ; Deferred.unit
    let is_closed { w; _ } = Pipe.is_closed w
    let close_finished { w; _ } = Pipe.closed w
  end

  let empty = {
    r = (fun _ -> return (Error (Error.of_string "disconnected"))) ;
    w = Pipe.create_writer (fun r -> Pipe.close_read r; Deferred.unit) ;
  }

  let read r w =
    Reader.peek r ~len:8 >>= fun _ ->
    (* let h = Reader.peek_available r ~len:8 in *)
    (* Log_async.debug (fun m -> m "%a" Hex.pp (Hex.of_string h)) >>= fun () -> *)
    Angstrom_async.parse hdr r >>= function
    | Error msg -> return (Error (Error.of_string msg))
    | Ok ({ big_endian; typ=_; compressed; len } as hdr) ->
      let msglen = Int32.to_int_exn len - 8 in
      Log_async.debug (fun m -> m "%a" Kx.pp_print_hdr hdr) >>= fun () ->
      (* let msg = Reader.peek_available r ~len:(msglen - 8) in *)
      (* Log_async.debug (fun m -> m "%d %a" (String.length msg) Hex.pp (Hex.of_string msg)) >>= fun () -> *)
      begin match compressed with
        | false -> Angstrom_async.parse (destruct ~big_endian w) r
        | true -> parse_compressed ~big_endian ~msglen w r
      end >>| function
      | Ok (Error msg) -> (Error (Error.of_string msg))
      | Error msg -> Error (Error.of_string msg)
      | Ok (Ok v) -> Ok v

  let send_client_msgs ?comp ?buf r w =
    Pipe.iter r ~f:begin fun (K { big_endian; typ; msg }) ->
      let serialized = construct ?comp ?buf ~big_endian ~typ:`Async typ msg in
      Writer.write_bigstring w serialized ;
      Log_async.debug (fun m -> m "@[%a@]" (Kx.pp typ) msg)
    end

  let connect ?comp ?(buf=Faraday.create 4096) url =
    let kx_read, client_write = Pipe.create () in
    let connected = Ivar.create () in
    let process _s _tls r w =
      Monitor.detach (Writer.monitor w) ;
      write_handshake w url ;
      Reader.read_char r >>= function
      | `Eof ->
        Ivar.fill connected (Or_error.error_string "EOF") ;
        Deferred.unit
      | `Ok c when c <> '\x03'->
        Ivar.fill connected (Or_error.errorf "Invalid handsharke %C" c) ;
        Deferred.unit
      | `Ok _ ->
        Ivar.fill connected (Ok { r = (fun w -> read r w) ; w = client_write }) ;
        send_client_msgs ?comp ~buf kx_read w in
    Deferred.any [
      Ivar.read connected ;
      (Monitor.try_with_or_error begin fun () ->
          Async_uri.with_connection url process
        end >>= function
       | Error e ->
         Pipe.close_read kx_read ;
         Log_async.err (fun m -> m "%a" Error.pp e) >>= fun () ->
         Deferred.Or_error.fail e
       | Ok () ->
         Pipe.close_read kx_read ;
         Deferred.Or_error.fail (Error.of_string "Connection terminated")) ;
    ]

  let with_connection ?comp ?buf url ~f =
    connect ?comp ?buf url >>= function
    | Error _ as e -> return e
    | Ok c ->
      Monitor.protect (fun () -> f c >>| fun v -> Ok v)
        ~finally:(fun () -> Pipe.close c.w ; Deferred.unit)

  module Persistent = struct
    include Persistent_connection_kernel.Make(T)
    let create' ~server_name ?on_event ?retry_delay ?comp ?buf =
      create ~server_name ?on_event ?retry_delay ~connect:(connect ?comp ?buf)
  end
end

type sf = {
  sf: 'a 'b. ('a w -> 'a -> 'b w -> 'b Deferred.Or_error.t)
}

let connect_sync ?comp ?big_endian ?(buf=Faraday.create 4096) url =
  Async_uri.connect url >>= fun (_s, _tls, r, w) ->
  write_handshake w url ;
  Reader.read_char r >>= function
  | `Eof  ->
    Deferred.Or_error.fail (Error.of_string "EOF")
  | `Ok c when c <> '\x03' ->
    Deferred.Or_error.fail (Error.createf "%d" (Char.to_int c))
  | `Ok _ ->
    Monitor.detach (Writer.monitor w) ;
    let f = { sf = fun wq q wr ->
        Monitor.try_with_or_error begin fun () ->
          let serialized = construct ?comp ?big_endian ~typ:`Sync ~buf wq q in
          Writer.write_bigstring w serialized ;
          Log_async.debug (fun m -> m "-> %a" (Kx.pp wq) q) >>= fun () ->
          let open Deferred.Result.Monad_infix in
          Angstrom_async.parse hdr r >>= fun { big_endian; typ=_; compressed; len } ->
          let msglen = Int32.to_int_exn len - 8 in
          begin match compressed with
            | false -> Angstrom_async.parse (destruct ~big_endian wr) r
            | true -> parse_compressed ~big_endian ~msglen wr r
          end
        end >>| function
        | Error e -> Error e
        | Ok (Error msg) -> Error (Error.of_string msg)
        | Ok (Ok (Error msg)) -> Error (Error.of_string msg)
        | Ok (Ok (Ok v)) -> Ok v
      } in
    return (Ok (f, r, w))

let with_connection_sync ?comp ?big_endian ?buf url ~f =
  connect_sync ?comp ?big_endian ?buf url >>= function
  | Error _ as e -> return e
  | Ok (f', r, w) ->
    Monitor.protect (fun () -> f f') ~finally:begin fun () ->
      Reader.close r >>= fun () ->
      Writer.close w
    end >>| fun v ->
    Ok v
