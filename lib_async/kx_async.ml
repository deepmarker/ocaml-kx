open Core
open Async
open Kx

let src = Logs.Src.create "kx.async"

module Log_async = (val Logs_async.src_log src : Logs_async.LOG)

let handle_chunk wbuf pos nb_to_read =
  if pos < 0 then invalid_arg "pos";
  if nb_to_read < 1 || nb_to_read > Bigstring.length wbuf - pos
  then invalid_arg "nb_to_read";
  let nb_to_read = ref nb_to_read in
  let wpos = ref pos in
  let inner buf ~pos ~len =
    let will_read = min len !nb_to_read in
    Bigstring.blito ~src:buf ~src_pos:pos ~src_len:will_read ~dst:wbuf ~dst_pos:!wpos ();
    if will_read = !nb_to_read
    then return (`Stop_consumed ((), will_read))
    else (
      wpos := !wpos + will_read;
      nb_to_read := !nb_to_read - will_read;
      return (`Consumed (will_read, `Need !nb_to_read)))
  in
  inner
;;

let parse_compressed ~big_endian ~msglen w r =
  let msglen = Int32.to_int_exn msglen in
  let module E = (val getmod big_endian : ENDIAN) in
  Angstrom_async.parse E.any_int32 r
  >>=? fun uncompLen ->
  let uncompLen = Int32.to_int_exn uncompLen in
  let compMsg = Bigstring.create msglen in
  Reader.read_one_chunk_at_a_time r ~handle_chunk:(handle_chunk compMsg 12 (msglen - 12))
  >>| fun _ ->
  let uncompMsg = Bigstringaf.create uncompLen in
  uncompress uncompMsg compMsg;
  Angstrom.parse_bigstring
    ~consume:Prefix
    (destruct ~big_endian w)
    (Bigstringaf.sub uncompMsg ~off:8 ~len:(uncompLen - 8))
;;

let write writer msg =
  let buf = Faraday.create 4096 in
  Kx.construct_msg buf msg;
  Faraday.close buf;
  let len = Faraday.pending_bytes buf in
  Writer.write_char writer (if Kx.big_endian msg then '\x00' else '\x01');
  Writer.write_char writer (char_of_msgtyp (Kx.typ msg));
  Writer.write_char writer '\x00';
  Writer.write_char writer '\x00';
  let lenbuf = Bigstring.create 4 in
  Bigstring.(if Kx.big_endian msg then unsafe_set_int32_be else unsafe_set_int32_le)
    lenbuf
    ~pos:0
    (8 + len);
  Writer.write_bigstring writer lenbuf;
  Faraday_async.serialize
    buf
    ~yield:(fun _ -> Scheduler.yield ())
    ~writev:(fun iovecs ->
      let q = Queue.create () in
      let len =
        List.fold_left iovecs ~init:0 ~f:(fun a iovec ->
          Queue.enqueue q (Obj.magic iovec);
          a + iovec.len)
      in
      Writer.schedule_iovecs writer q;
      Writer.flushed writer >>| fun () -> `Ok len)
;;

module T = struct
  module Address = Uri_sexp

  type t =
    { r : 'a. 'a Kx.w -> 'a Deferred.Or_error.t
    ; w : Kx.t Pipe.Writer.t
    }

  let close { w; _ } =
    Pipe.close w;
    Pipe.closed w
  ;;

  let is_closed { w; _ } = Pipe.is_closed w
  let close_finished { w; _ } = Pipe.closed w
end

include T

let empty =
  { r = (fun _ -> failwith "disconnected")
  ; w =
      Pipe.create_writer (fun r ->
        Pipe.close_read r;
        Deferred.unit)
  }
;;

let process ?(monitor = Monitor.current ()) url r w =
  let handshake () =
    Writer.write
      w
      (Printf.sprintf
         "%s:%s\x03\x00"
         (Option.value ~default:"" (Uri.user url))
         (Option.value ~default:"" (Uri.password url)));
    Reader.read_char r
    >>= function
    | `Eof -> failwith "kx handshake returned EOF"
    | `Ok '\x03' -> Deferred.unit
    | `Ok c -> failwithf "Invalid handshake %C" c ()
  in
  let read w =
    Reader.peek r ~len:8
    >>= function
    | `Eof -> Deferred.Result.fail "kx read EOF"
    | `Ok _ ->
      Angstrom_async.parse hdr r
      >>=? fun ({ big_endian; typ = _; compressed; len } as hdr) ->
      Log_async.debug (fun m -> m "%a" Kx.pp_print_hdr hdr)
      >>= fun () ->
      (match compressed with
       | false -> Angstrom_async.parse (destruct ~big_endian w) r
       | true -> parse_compressed ~big_endian ~msglen:len w r)
  in
  let protected_read wit =
    Monitor.try_with_join_or_error ~extract_exn:true (fun () ->
      Deferred.Result.map_error ~f:Error.of_string (read wit))
    >>= function
    | Error _ as e -> Writer.close w >>= fun () -> return e
    | Ok _ as o -> return o
  in
  let send_client_msgs r =
    Pipe.iter ~continue_on_error:false r ~f:(fun msg ->
      try_with ~extract_exn:true (fun () ->
        write w msg >>= fun () -> Log_async.debug (fun m -> m "%a" Kx.pp_hum msg))
      >>| function
      | Ok _ -> ()
      | Error exn -> Monitor.send_exn monitor exn)
  in
  (* Above are helper functions *)
  handshake ()
  >>= fun () ->
  Monitor.detach_and_iter_errors (Writer.monitor w) ~f:(fun exn ->
    Writer.close w >>> fun () -> Monitor.send_exn monitor exn);
  (* *)
  let client_write =
    Pipe.create_writer (fun r ->
      Deferred.any [ Writer.close_started w; send_client_msgs r ])
  in
  don't_wait_for (Pipe.closed client_write >>= fun () -> Writer.close w);
  return { r = protected_read; w = client_write }
;;

module Persistent = struct
  include Persistent_connection_kernel.Make (T)

  let create' ?monitor =
    create ~connect:(fun addr ->
      Monitor.try_with_or_error (fun () ->
        Async_uri.connect addr >>= fun { r; w; _ } -> process ?monitor addr r w))
  ;;
end
