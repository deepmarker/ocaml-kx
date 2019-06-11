open Core
open Async
open Kx

let src = Logs.Src.create "kx.async"
module Log = (val Logs.src_log src : Logs.LOG)
module Log_async = (val Logs_async.src_log src : Logs_async.LOG)

type t = K : 'a w * 'a -> t
let create w v = K (w, v)

(* TODO: rewrite with `write_direct` *)
let write_iovec w iovec =
  List.fold_left iovec ~init:0 ~f:begin fun a { Faraday.buffer ; off ; len } ->
    Writer.write_bigstring w buffer ~pos:off ~len ;
    a+len
  end

let rec flush w buf =
  match Faraday.operation buf with
  | `Close -> raise Exit
  | `Yield -> Deferred.unit
  | `Writev iovecs ->
    let nb_written = write_iovec w iovecs in
    Faraday.shift buf nb_written ;
    flush w buf

let connect ?(buf=Faraday.create 4096) url =
  let hdr = Faraday.create 8 in
  (* let client_read, kx_write = Pipe.create () in *)
  let connected = Ivar.create () in
  don't_wait_for @@ Async_uri.with_connection url begin fun _s _tls r w ->
    Writer.write w
      (Printf.sprintf "%s:%s\x03\x00"
         (Option.value ~default:"" (Uri.user url))
         (Option.value ~default:"" (Uri.password url))) ;
    Reader.read_char r >>= function
    | `Ok '\x03' ->
      let kx_read, client_write = Pipe.create () in
      Ivar.fill connected (Ok client_write) ;
      Pipe.iter ~continue_on_error:false kx_read ~f:begin fun (K (wit, msg)) ->
        construct ~hdr ~payload:buf wit msg ;
        flush w hdr >>= fun () ->
        flush w buf >>= fun () ->
        Log_async.debug (fun m -> m "-> %a" (Kx.pp wit) msg)
      end >>= fun () ->
      Pipe.close client_write ;
      Deferred.unit
    | `Ok c ->
      Ivar.fill connected (Error (Format.asprintf "Remote server only supports protocol %C" c)) ;
      Deferred.unit
    | `Eof ->
      Ivar.fill connected (Error "Remote server closed connection") ;
      Deferred.unit
  end ;
  Ivar.read connected

let with_connection ?buf url ~f =
  connect ?buf url >>= function
  | Error _ as e -> return e
  | Ok w ->
  Monitor.protect (fun () -> f w >>| fun v -> Ok v)
    ~finally:(fun () -> Pipe.close w ; Deferred.unit)
