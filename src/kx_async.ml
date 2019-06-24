open Core
open Async
open Kx

let src = Logs.Src.create "kx.async"
module Log = (val Logs.src_log src : Logs.LOG)
module Log_async = (val Logs_async.src_log src : Logs_async.LOG)

type t = K : 'a w * 'a -> t
let create w v = K (w, v)

let rec write_iovec w { Faraday.buffer ; off ; len } =
  Writer.write_direct w ~f:begin fun buf ~pos ~len:len' ->
    let nbwritten = min len len' in
    Bigstring.blit ~src:buffer ~src_pos:off ~dst:buf ~dst_pos:pos ~len:nbwritten ;
    nbwritten, nbwritten
  end |> function
  | None -> failwith "write_iovec stopped"
  | Some n when n = len -> ()
  | Some n -> write_iovec w { Faraday.buffer ; off = off + n ; len = len - n }

let rec flush w buf =
  let write_iovec iovec =
    List.fold_left iovec ~init:0 ~f:begin fun a ({ Faraday.len; _ } as iovec) ->
      write_iovec w iovec ;
      a+len
    end in
  match Faraday.operation buf with
  | `Close -> raise Exit
  | `Yield -> Deferred.unit
  | `Writev iovecs ->
    let nb_written = write_iovec iovecs in
    Faraday.shift buf nb_written ;
    flush w buf

type error = [
  | `ConnectionTerminated
  | `Exn of exn
  | `ProtoError of int
  | `Eof
]

let pp_print_error ppf = function
  | `ProtoError ver ->
    Format.fprintf ppf "Remote server only supports protocol version %d" ver
  | `Eof ->
    Format.fprintf ppf "Remote server closed connection"
  | `ConnectionTerminated ->
    Format.fprintf ppf "Connection terminated"
  | `Exn exn ->
    Format.fprintf ppf "%a" Exn.pp exn

let connect ?(buf=Faraday.create 4096) url =
  let hdr = Faraday.create 8 in
  let kx_read, client_write = Pipe.create () in
  let connected = Ivar.create () in
  let inner () =
    Async_uri.with_connection url begin fun _s _tls r w ->
      Monitor.detach (Writer.monitor w) ;
      Writer.write w
        (Printf.sprintf "%s:%s\x03\x00"
           (Option.value ~default:"" (Uri.user url))
           (Option.value ~default:"" (Uri.password url))) ;
      Reader.read_char r >>= function
      | `Ok '\x03' ->
        begin try_with begin fun () ->
            let f w = Angstrom_async.parse (destruct w) r in
            Ivar.fill connected (Ok (f, client_write)) ;
            Pipe.iter kx_read ~f:begin fun (K (wit, msg)) ->
              construct ~hdr ~payload:buf wit msg ;
              flush w hdr >>= fun () ->
              flush w buf >>= fun () ->
              Log_async.debug (fun m -> m "-> %a" (Kx.pp wit) msg)
            end
          end >>| fun _ ->
          Pipe.close_read kx_read
        end
      | `Ok c ->
        Ivar.fill connected (Error (`ProtoError (Char.to_int c))) ;
        Deferred.unit
      | `Eof ->
        Ivar.fill connected (Error (`Eof)) ;
        Deferred.unit
    end in
  let th = try_with ~extract_exn:true inner >>| fun e ->
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

let with_connection ?buf url ~f =
  connect ?buf url >>= function
  | Error _ as e -> return e
  | Ok (f', w) ->
  Monitor.protect (fun () -> f f' w >>| fun v -> Ok v)
    ~finally:(fun () -> Pipe.close w ; Deferred.unit)
