open Core
open Async
open Kx

let src = Logs.Src.create "kx.async"
module Log = (val Logs.src_log src : Logs.LOG)
module Log_async = (val Logs_async.src_log src : Logs_async.LOG)

type t = K : [`Big | `Little] * [`Sync | `Async] * 'a w * 'a -> t
let create ?(endianness=`Little) ?(typ=`Async) w v = K (endianness, typ, w, v)

let rec flush w buf =
  match Faraday.operation buf with
  | `Close -> raise Exit
  | `Yield -> Deferred.unit
  | `Writev iovecs ->
    let nb_written =
      List.fold_left iovecs ~init:0 ~f:begin fun a { Faraday.buffer ; off ; len } ->
        Writer.write_bigstring w buffer ~pos:off ~len ;
        a+len
      end in
    Faraday.shift buf nb_written ;
    flush w buf

type error = [
  | `Angstrom of string
  | `ConnectionTerminated
  | `Exn of exn
  | `ProtoError of int
  | `Eof
]

let pp_print_error ppf = function
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

let connect_async ?(buf=Faraday.create 4096) url =
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
            let f w =
              Reader.peek r ~len:1 >>= fun _ ->
              let msg = Reader.peek_available r ~len:4096 in
              Log.debug (fun m -> m "--> %S" msg);
              Angstrom_async.parse (destruct w) r in
            Ivar.fill connected (Ok (f, client_write)) ;
            Pipe.iter kx_read ~f:begin fun (K (endianness, typ, wit, msg)) ->
              construct ~endianness ~typ ~hdr ~payload:buf wit msg ;
              flush w hdr >>= fun () ->
              flush w buf >>= fun () ->
              Log_async.debug begin fun m ->
                m "@[%a@]" (Kx.pp wit) msg
              end
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

let with_connection_async ?buf url ~f =
  connect_async ?buf url >>= function
  | Error _ as e -> return e
  | Ok (f', w) ->
    Monitor.protect (fun () -> f f' w >>| fun v -> Ok v)
      ~finally:(fun () -> Pipe.close w ; Deferred.unit)

type f = {
  f: 'a 'b. ('a w -> 'a -> 'b w -> (hdr * 'b, error) result Deferred.t)
}

let connect_sync ?endianness ?(buf=Faraday.create 4096) url =
  let hdr = Faraday.create 8 in
  Async_uri.connect url >>= fun (_s, _tls, r, w) ->
  Writer.write w
    (Printf.sprintf "%s:%s\x03\x00"
       (Option.value ~default:"" (Uri.user url))
       (Option.value ~default:"" (Uri.password url))) ;
  Reader.read_char r >>= function
  | `Ok '\x03' ->
    Monitor.detach (Writer.monitor w) ;
    let f = { f = fun wq q wr ->
      try_with begin fun () ->
        construct ?endianness ~typ:`Sync ~hdr ~payload:buf wq q ;
        flush w hdr >>= fun () ->
        flush w buf >>= fun () ->
        Log_async.debug (fun m -> m "-> %a" (Kx.pp wq) q) >>= fun () ->
        Angstrom_async.parse (destruct wr) r
      end >>| function
      | Error e -> Error (`Exn e)
      | Ok (Error msg) -> Error (`Angstrom msg)
      | Ok (Ok v) -> Ok v
      } in
    return (Ok (f, r, w))
  | `Ok c -> return (Error (`ProtoError (Char.to_int c)))
  | `Eof  -> return (Error (`Eof))

let with_connection_sync ?endianness ?buf url ~f =
  connect_sync ?endianness ?buf url >>= function
  | Error _ as e -> return e
  | Ok (f', r, w) ->
    Monitor.protect (fun () -> f f') ~finally:begin fun () ->
      Reader.close r >>= fun () ->
      Writer.close w
    end >>| fun v ->
    Ok v
