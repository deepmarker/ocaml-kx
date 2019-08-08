open Core
open Async
open Kx

let url = Uri.make ~scheme:"" ~host:"localhost" ()

let main port =
  let url = Uri.with_port url (Some port) in
  Kx_async.with_connection_sync url ~f:begin fun { sf } ->
    Pipe.iter Reader.(pipe @@ Lazy.force stdin) ~f:begin fun expr ->
      let expr = String.chop_suffix_exn expr ~suffix:"\n" in
      sf (s char) expr (a long) >>= function
      | Ok (hdr, v) -> Logs_async.app (fun m -> m "%a %Ld" Kx.pp_print_hdr hdr v)
      | Error e -> Logs_async.err (fun m -> m "%a" Kx_async.pp_print_error e)
    end;
  end >>= fun _ ->
  Deferred.unit

let () =
  Command.async ~summary:"Kdb+ client" begin
    let open Command.Let_syntax in
    [%map_open
      let () = Logs_async_reporter.set_level_via_param None
      and port = anon ("port" %: int) in
      fun () ->
        Logs.set_reporter (Logs_async_reporter.reporter ()) ;
        main port
    ] end |>
  Command.run
