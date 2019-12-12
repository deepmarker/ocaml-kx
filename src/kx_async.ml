open Core
open Async
open Kx

let src = Logs.Src.create "kx.async"
module Log = (val Logs.src_log src : Logs.LOG)
module Log_async = (val Logs_async.src_log src : Logs_async.LOG)

type msg = K : { big_endian:bool; typ:'a w;  msg:'a } -> msg
let create ?(big_endian=Sys.big_endian) typ msg = K { big_endian; typ; msg }

let pp_serialized ppf (K { big_endian; typ; msg }) =
  let serialized = construct_bigstring ~big_endian ~typ:Async typ msg in
  Format.fprintf ppf "0x%a" Hex.pp (Hex.of_bigstring serialized)

let parse w r =
  Deferred.Result.map_error ~f:Error.of_string (Angstrom_async.parse w r)

let handle_chunk wbuf pos nb_to_read =
  if pos < 0 then invalid_arg "pos" ;
  if nb_to_read < 1 ||
     nb_to_read > (Bigstring.length wbuf - pos) then invalid_arg "nb_to_read" ;
  let nb_to_read = ref nb_to_read in
  let wpos = ref pos in
  let inner buf ~pos ~len =
    let will_read = min len !nb_to_read in
    Bigstring.blito
      ~src:buf ~src_pos:pos ~src_len:will_read
      ~dst:wbuf ~dst_pos:!wpos () ;
    if will_read = !nb_to_read then
      return (`Stop_consumed ((), will_read))
    else begin
      wpos := !wpos + will_read ;
      nb_to_read := !nb_to_read - will_read ;
      return (`Consumed (will_read, `Need !nb_to_read))
    end
  in
  inner

let parse_compressed ~big_endian ~msglen w r =
  let msglen = Int32.to_int_exn msglen in
  let module E = (val (getmod big_endian) : ENDIAN) in
  parse E.any_int32 r >>=? fun uncompLen ->
  let uncompLen = Int32.to_int_exn uncompLen in
  let compMsg = Bigstring.create msglen in
  Reader.read_one_chunk_at_a_time r
    ~handle_chunk:(handle_chunk compMsg 12 (msglen - 12)) >>= fun _ ->
  let uncompMsg = Bigstringaf.create uncompLen in
  uncompress uncompMsg compMsg ;
  let res =
    Angstrom.parse_bigstring (destruct ~big_endian w)
      (Bigstringaf.sub uncompMsg ~off:8 ~len:(uncompLen-8)) in
  return (Result.(map_error res ~f:(Error.of_string)))

let write_handshake w url =
  Writer.write w
    (Printf.sprintf "%s:%s\x03\x00"
       (Option.value ~default:"" (Uri.user url))
       (Option.value ~default:"" (Uri.password url)))

let _write ?(big_endian=Sys.big_endian) ~typ ?(buf=Faraday.create 4096) writer w x =
  let module FE =
    (val Faraday.(if big_endian then (module BE : FE) else (module LE : FE))) in
  construct (module FE) buf w x ;
  let len = Faraday.pending_bytes buf in
  let hdrbuf = Bytes.make 8 '\x00' in
  Bytes.set hdrbuf 0 (if big_endian then '\x00' else '\x01') ;
  Bytes.set hdrbuf 1 (char_of_msgtyp typ) ;
  EndianBytes.BigEndian.set_int32 hdrbuf 4 (Int32.of_int_exn (8 + len)) ;
  Writer.write_bytes writer hdrbuf ;
  Faraday_async.serialize buf
    ~writev:(Faraday_async.writev_of_fd (Writer.fd writer))
    ~yield:(fun _ -> Writer.flushed writer)

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
  r = (fun _ -> failwith "disconnected") ;
  w = Pipe.create_writer (fun r -> Pipe.close_read r; Deferred.unit) ;
}

let read r w =
  Reader.peek r ~len:8 >>= fun _ ->
  Deferred.Result.map_error ~f:Error.of_string
    (Angstrom_async.parse hdr r) >>=? fun ({ big_endian; typ=_; compressed; len } as hdr) ->
  Log_async.debug (fun m -> m "%a" Kx.pp_print_hdr hdr) >>= fun () ->
  begin match compressed with
    | false -> parse (destruct ~big_endian w) r
    | true ->  parse_compressed ~big_endian ~msglen:len w r
  end

let send_client_msgs ?buf r w =
  Pipe.iter r ~f:begin fun (K { big_endian; typ; msg }) ->
    (* write ~big_endian ~typ:Async ?buf w typ msg >>= fun () -> *)
    let serialized = construct_bigstring ?buf ~big_endian ~typ:Async typ msg in
    Writer.write_bigstring w serialized ;
    Log_async.debug (fun m -> m "@[%a@]" (Kx.pp typ) msg)
  end

let connect ?(buf=Faraday.create 4096) url =
  let kx_read, client_write = Pipe.create () in
  let connected = Ivar.create () in
  let process _s _tls r w =
    write_handshake w url ;
    Reader.read_char r >>= function
    | `Eof ->
      Ivar.fill connected (Or_error.error_string "EOF") ;
      Deferred.unit
    | `Ok c when c <> '\x03'->
      Ivar.fill connected (Or_error.errorf "Invalid handsharke %C" c) ;
      Deferred.unit
    | `Ok _ ->
      Monitor.detach_and_iter_errors (Writer.monitor w) ~f:begin fun exn ->
        Writer.close w >>> fun () ->
        Log.err (fun m -> m "%a" Exn.pp exn)
      end ;
      let protected_read wit =
        Monitor.try_with_join_or_error (fun () -> read r wit) >>= function
        | Error e -> Writer.close w >>= fun () -> return (Error e)
        | Ok v -> return (Ok v)
      in
      Ivar.fill connected
        (Ok { r = protected_read ;
              w = client_write }) ;
      send_client_msgs ~buf kx_read w in
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

let with_connection ?buf url ~f =
  connect ?buf url >>=? fun c ->
  Monitor.try_with_join_or_error (fun () -> f c) >>| fun res ->
  Pipe.close c.w ;
  res

module Persistent = struct
  include Persistent_connection_kernel.Make(T)

  let with_current_connection c ~f =
    match current_connection c with
    | None -> Deferred.Or_error.error_string "No current connection available"
    | Some c -> Monitor.try_with_join_or_error (fun () -> f c)

  let create' ~server_name ?on_event ?retry_delay ?buf =
    create ~server_name ?on_event ?retry_delay ~connect:(connect ?buf)
end
