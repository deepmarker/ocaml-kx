open Core
open Async
open Kx

let src = Logs.Src.create "kx.async"
module Log = (val Logs.src_log src : Logs.LOG)
module Log_async = (val Logs_async.src_log src : Logs_async.LOG)

type t = K : bool * [`Sync | `Async] * 'a w * 'a -> t
let create ?(big_endian=Sys.big_endian) ?(typ=`Async) w v = K (big_endian, typ, w, v)

type error = [
  | `Q of string
  | `Angstrom of string
  | `ConnectionTerminated
  | `Exn of exn
  | `ProtoError of int
  | `Eof
] [@@deriving sexp_of]

let pp_print_error ppf = function
  | `Q msg ->
    Format.fprintf ppf "Q error: %s" msg
  | `Angstrom msg ->
    Format.fprintf ppf "Parsing error: %s" msg
  | `ProtoError ver ->
    Format.fprintf ppf "Remote server only supports protocol version %d" ver
  | `Eof ->
    Format.fprintf ppf "Remote server closed connection"
  | `ConnectionTerminated ->
    Format.fprintf ppf "Connection terminated"
  | `Exn exn ->
    Format.fprintf ppf "%a" Exn.pp exn

let fail err = failwith (Format.asprintf "%a" pp_print_error err)
let fail_on_error = function
  | Ok v -> v
  | Error e -> fail e

type af = {
  af: 'a. 'a w -> ('a, [`Q of string | `Angstrom of string]) result Deferred.t
}

let parse_compressed ~big_endian ~msglen w r =
  let module E = (val (getmod big_endian) : ENDIAN) in
  let open Deferred.Result.Monad_infix in
  Angstrom_async.parse E.any_int32 r >>= fun uncompLen ->
  let uncompLen = Int32.to_int_exn uncompLen in
  Angstrom_async.parse (Angstrom.take_bigstring (msglen-12)) r >>= fun compMsg ->
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

let connect_async ?comp ?(buf=Faraday.create 4096) url =
  let kx_read, client_write = Pipe.create () in
  let connected = Ivar.create () in
  let process _s _tls r w =
    Monitor.detach (Writer.monitor w) ;
    write_handshake w url ;
    Reader.read_char r >>= function
    | `Ok '\x03' ->
      begin try_with begin fun () ->
          let af w =
            Reader.peek r ~len:1 >>= fun _ ->
            Angstrom_async.parse hdr r >>= function
            | Error msg -> return (Error (`Angstrom msg))
            | Ok ({ big_endian; typ=_; compressed; len } as hdr) ->
              Log_async.debug (fun m -> m "%a" Kx.pp_print_hdr hdr) >>= fun () ->
              begin match compressed with
                | false -> Angstrom_async.parse (destruct ~big_endian w) r
                | true -> parse_compressed ~big_endian ~msglen:(Int32.to_int_exn len) w r
              end >>| function
              | Ok (Error msg) -> (Error (`Q msg))
              | Error msg -> Error (`Angstrom msg)
              | Ok (Ok v) -> Ok v
          in
          Ivar.fill connected (Ok ({ af }, client_write)) ;
          Pipe.iter kx_read ~f:begin fun (K (big_endian, typ, wit, msg)) ->
            let serialized = construct ?comp ~big_endian ~typ ~buf wit msg in
            Writer.write_bigstring w serialized ;
            Log_async.debug (fun m -> m "@[%a@]" (Kx.pp wit) msg)
          end
        end >>| fun _ ->
        Pipe.close_read kx_read
      end
    | `Ok c ->
      Ivar.fill connected (Error (`ProtoError (Char.to_int c))) ;
      Deferred.unit
    | `Eof ->
      Ivar.fill connected (Error (`Eof)) ;
      Deferred.unit in
  let th = try_with ~extract_exn:true begin fun () ->
      Async_uri.with_connection url process
    end >>| fun e ->
    Pipe.close_read kx_read ;
    e in
  Deferred.any [
    (th >>= function
      | Error exn ->
        Log_async.err (fun m -> m "%a" Exn.pp exn) >>| fun () ->
        Error (`Exn exn)
      | Ok () -> return (Error `ConnectionTerminated)) ;
    Ivar.read connected ;
  ]

let with_connection_async ?comp ?buf url ~f =
  connect_async ?comp ?buf url >>= function
  | Error _ as e -> return e
  | Ok (f', w) ->
    Monitor.protect (fun () -> f f' w >>| fun v -> Ok v)
      ~finally:(fun () -> Pipe.close w ; Deferred.unit)

type sf = {
  sf: 'a 'b. ('a w -> 'a -> 'b w -> ('b, error) result Deferred.t)
}

let connect_sync ?comp ?big_endian ?(buf=Faraday.create 4096) url =
  Async_uri.connect url >>= fun (_s, _tls, r, w) ->
  write_handshake w url ;
  Reader.read_char r >>= function
  | `Ok '\x03' ->
    Monitor.detach (Writer.monitor w) ;
    let f = { sf = fun wq q wr ->
        try_with begin fun () ->
          let serialized = construct ?comp ?big_endian ~typ:`Sync ~buf wq q in
          Writer.write_bigstring w serialized ;
          Log_async.debug (fun m -> m "-> %a" (Kx.pp wq) q) >>= fun () ->
          let open Deferred.Result.Monad_infix in
          Angstrom_async.parse hdr r >>= fun { big_endian; typ=_; compressed; len } ->
          begin match compressed with
            | false -> Angstrom_async.parse (destruct ~big_endian wr) r
            | true -> parse_compressed ~big_endian ~msglen:(Int32.to_int_exn len) wr r
          end
        end >>| function
        | Error e -> Error (`Exn e)
        | Ok (Error msg) -> Error (`Angstrom msg)
        | Ok (Ok (Error msg)) -> Error (`Q msg)
        | Ok (Ok (Ok v)) -> Ok v
      } in
    return (Ok (f, r, w))
  | `Ok c -> return (Error (`ProtoError (Char.to_int c)))
  | `Eof  -> return (Error (`Eof))

let with_connection_sync ?comp ?big_endian ?buf url ~f =
  connect_sync ?comp ?big_endian ?buf url >>= function
  | Error _ as e -> return e
  | Ok (f', r, w) ->
    Monitor.protect (fun () -> f f') ~finally:begin fun () ->
      Reader.close r >>= fun () ->
      Writer.close w
    end >>| fun v ->
    Ok v
