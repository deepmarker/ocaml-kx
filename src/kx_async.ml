open Core
open Async

let src = Logs.Src.create "kx.async"

let of_file_descr fd =
  let client_read, to_client = Pipe.create () in
  let from_client, client_write = Pipe.create () in
  let rec do_read () =
    Fd.ready_to fd `Read >>= function
    | `Bad_fd
    | `Closed ->
      Pipe.close to_client ;
      Deferred.unit
    | `Ready ->
      let k = Fd.syscall_exn ~nonblocking:true fd Kx.kread in
      Pipe.write to_client k >>=
      do_read
  in
  let do_write () =
    Pipe.iter_without_pushback
      ~continue_on_error:false from_client ~f:begin fun (msg, a) ->
      let pp_sep ppf () = Format.pp_print_string ppf " " in
      Logs.debug ~src begin fun m ->
        m "-> %s %a" msg
          (Format.pp_print_list ~pp_sep Kx.pp)
          (Array.to_list a)
      end ;
      let a = Array.map ~f:Kx.pack a in
      Fd.syscall_exn ~nonblocking:true fd (fun fd -> Kx.kn fd msg a)
    end in
  don't_wait_for begin
    Deferred.any_unit [
      do_read () ;
      Monitor.try_with do_write >>= function
      | Ok () ->
        Logs_async.err ~src (fun m -> m "Write pipe closed by client") >>= fun () ->
        Deferred.unit
      | Error e ->
        Logs_async.err ~src (fun m -> m "ERROR %a" Exn.pp e) >>| fun () ->
        Pipe.close_read from_client
    ]
  end ;
  client_read, client_write

let connect ?timeout ?capability url =
  match Kx.connect ?timeout ?capability url with
  | Error e -> Error e
  | Ok fd ->
    let fd = Fd.create (Socket `Active)
        fd (Info.of_string "fd from Kx.connect") in
    Ok (of_file_descr fd)

let with_connection ?timeout ?capability url ~f =
  match Kx.connect ?timeout ?capability url with
  | Error e -> return (Error e)
  | Ok fd ->
    let fd = Fd.create (Socket `Active)
        fd (Info.of_string "fd from Kx.connect") in
    let r, w = of_file_descr fd in
    f r w >>| fun a ->
    (try Kx.kclose (Fd.file_descr_exn fd) with _ -> ()) ;
    Ok a
