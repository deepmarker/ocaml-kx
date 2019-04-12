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
    Pipe.iter_without_pushback ~continue_on_error:false from_client ~f:begin function
      | msg, [||] ->
        if not (Fd.syscall_exn ~nonblocking:true fd (fun fd -> Kx.k0 fd msg)) then
          raise Exit
      | msg, [|a|] ->
        if not (Fd.syscall_exn ~nonblocking:true fd (fun fd -> Kx.k1 fd msg a)) then
          raise Exit
      | msg, [|a; b|] ->
        if not (Fd.syscall_exn ~nonblocking:true fd (fun fd -> Kx.k2 fd msg a b)) then
          raise Exit
      | msg, [|a; b; c|] ->
        if not (Fd.syscall_exn ~nonblocking:true fd (fun fd -> Kx.k3 fd msg a b c)) then
          raise Exit
      | _ -> invalid_arg "more than 3 arguments in not supported"
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
  return (client_read, client_write)

let khpu ~host ~port ~username =
  match Kx.khpu ~host ~port ~username with
  | Error e -> Error e
  | Ok fd ->
    let fd = Fd.create (Socket `Active)
        fd (Info.of_string "fd from Kx.khpu") in
    Ok (of_file_descr fd)

let khpun ~host ~port ~username ~timeout_ms =
  match Kx.khpun ~host ~port ~username ~timeout_ms with
  | Error e -> Error e
  | Ok fd ->
    let fd = Fd.create (Socket `Active)
        fd (Info.of_string "fd from Kx.khpun") in
    Ok (of_file_descr fd)

