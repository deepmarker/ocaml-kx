open Core
open Async_kernel
open Async_unix

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
    Pipe.iter_without_pushback ~continue_on_error:false from_client ~f:begin fun (msg, a) ->
      if not (Fd.syscall_exn ~nonblocking:true fd (fun fd -> Kx.kn fd msg a)) then
        raise Exit
    end in
  don't_wait_for begin
    Deferred.all_unit [
      do_read () ;
      Monitor.try_with ~extract_exn:true do_write >>| function
      | Error _ ->
        Pipe.close_read from_client
      | Ok () -> ()
    ]
  end ;
  client_read, client_write

let connect ?credentials ?timeout ?capability ~host ~port () =
  match Kx.connect ?credentials ?timeout ?capability ~host ~port () with
  | Error e -> Error e
  | Ok fd ->
    let fd = Fd.create (Socket `Active)
        fd (Info.of_string "fd from Kx.connect") in
    Ok (of_file_descr fd)

let with_connection ?credentials ?timeout ?capability ~host ~port f =
  match Kx.connect ?credentials ?timeout ?capability ~host ~port () with
  | Error e -> return (Error e)
  | Ok fd ->
    let fd = Fd.create (Socket `Active)
        fd (Info.of_string "fd from Kx.connect") in
    let r, w = of_file_descr fd in
    f r w >>| fun a ->
    (try Kx.kclose (Fd.file_descr_exn fd) with _ -> ()) ;
    Ok a
