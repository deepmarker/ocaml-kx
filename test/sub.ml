open Core
open Async

let url = Uri.make ~scheme:"" ~host:"localhost" ()

type trade = {
  sym: string ;
  side: char ;
  size: int64 ;
  price: float ;
} [@@deriving sexp]

let random_trade () =
  let sym = "XBTUSD" in
  let side = if Random.bool () then 'b' else 'a' in
  let size = Random.int64 1000L in
  let price = 9000. +. Random.float 100. in
  { sym ; side ; size ; price }

let pp_trade ppf t =
  Format.fprintf ppf "%a" Sexplib.Sexp.pp (sexp_of_trade t)

let trade =
  let open Kx in
  conv
    (fun { sym ; side; size; price } -> (sym, side, size, price))
    (fun (sym, side, size, price) -> { sym ; side; size; price })
    (t4 (a sym) (a char) (a long) (a float))

let pubmsg = Kx.(t3 (a sym) (a sym) trade)
let submsg = Kx.(t3 (a sym) (a sym) (a sym))

let updmsg =
  let open Kx in
  let v = t5 (v timespan) (v sym) (v char) (v long) (v float) in
  let row = table v in
  t3 (a sym) (a sym) row

let add_random_trades w =
  let t = random_trade () in
  Logs.app (fun m -> m "%a" pp_trade t) ;
  Pipe.write_without_pushback_if_open w
    (Kx_async.create pubmsg (".u.upd", "trade", random_trade ()))

let main port =
  let url = Uri.with_port url (Some port) in
  let sub = Kx_async.create submsg (".u.sub", "trade", "") in
  Kx_async.Async.with_connection url ~f:begin fun { r; w } ->
    Clock_ns.every
      (Time_ns.Span.of_int_sec 1)
      (fun () -> add_random_trades w) ;
    Pipe.write w sub >>= fun () ->
    let rec inner () =
      r updmsg >>= function
      | Error e -> Error.raise e
      | Ok a ->
        Logs.app (fun m ->   m "%a" (Kx.pp updmsg) a) ;
        inner ()
    in
    inner ()
  end >>= fun _ ->
  Deferred.unit

let () =
  Command.async ~summary:"Feed subscriber" begin
    let open Command.Let_syntax in
    [%map_open
      let () = Logs_async_reporter.set_level_via_param None
      and port = anon ("port" %: int) in
      fun () ->
        Logs.set_reporter (Logs_async_reporter.reporter ()) ;
        main port
    ] end |>
  Command.run
