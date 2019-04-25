open Kx
open Alcotest

let pack_unpack
  : type a. string -> a w -> a -> unit = fun name w a ->
  let v = construct w a in
  let _ = Format.asprintf "%a" Kx.pp v in
  let v_serialized = to_string v in
  let vv_parsed = of_string_exn w v_serialized in
  let vv_serialized = to_string vv_parsed in
  check string (name ^ "_serial") v_serialized vv_serialized ;
  check (testable Kx.pp Kx.equal) (name ^ "_parse") v vv_parsed ;
  match destruct w v with
  | Error msg -> failwith msg
  | Ok vv ->
    let vvv = construct w vv in
    check (testable Kx.pp Kx.equal) (name ^ "_destruct") v vvv

let pack_unpack_atom () =
  let open Kx in
  pack_unpack "bool" (a bool) true ;
  pack_unpack "guid" (a guid) Uuidm.nil ;
  pack_unpack "byte" (a byte) '\x00' ;
  pack_unpack "short" (a short) 0 ;
  pack_unpack "int" (a int) 0l ;
  pack_unpack "long" (a long) 0L ;
  pack_unpack "real" (a real) 0. ;
  pack_unpack "float" (a float) 0. ;
  pack_unpack "char" (a char) '\x00' ;
  pack_unpack "symbol" (a sym) "" ;
  pack_unpack "timestamp" (a timestamp) Ptime.epoch ;
  pack_unpack "month" (a month) (2018, 4, 0) ;
  pack_unpack "month" (a month) (2019, 1, 0) ;
  pack_unpack "month" (a month) (2019, 2, 0) ;
  pack_unpack "date" (a date) (2017, 10, 6) ;
  pack_unpack "timespan" (a timespan) { time = (0, 0, 0), 0 ; ns = 0 } ;
  pack_unpack "minute" (a minute) ((0, 0, 0), 0) ;
  pack_unpack "second" (a second) ((0, 0, 0), 0) ;
  pack_unpack "time" (a time) { time = ((0, 0, 0), 0) ; ms = 0 };
  ()

let pack_unpack_vect () =
  let open Kx in
  pack_unpack "vect bool" (v bool) [|true; false|] ;
  pack_unpack "vect guid" (v guid) [|Uuidm.nil; Uuidm.nil|] ;
  pack_unpack "vect byte" (s byte) "\x00\x01\x02" ;
  pack_unpack "vect short" (v short) [|0;1;2|] ;
  pack_unpack "vect int" (v int) [|0l;1l;2l;Int32.max_int;Int32.min_int|] ;
  pack_unpack "vect long" (v long) [|0L;1L;2L;Int64.max_int;Int64.min_int|] ;
  pack_unpack "vect real" (v real) [|0.;1.;nan;infinity;neg_infinity|] ;
  pack_unpack "vect float" (v float) [|0.;1.;nan;infinity;neg_infinity|] ;
  pack_unpack "vect char" (s char) "bleh" ;
  pack_unpack "vect symbol" (v sym) [|"machin"; "truc"; "chouette"|] ;
  pack_unpack "vect timestamp" (v timestamp) [|Ptime.epoch; Ptime.epoch|] ;
  pack_unpack "vect month" (v month) [|2019, 1, 0 ; 2019, 2, 0|] ;
  pack_unpack "vect date" (v date) [|2019, 1, 1; 2019, 1, 2|] ;
  pack_unpack "vect timespan" (v timespan) [||] ;
  pack_unpack "vect minute" (v minute) [||] ;
  pack_unpack "vect second" (v second) [||] ;
  pack_unpack "vect time" (v time) [||] ;
  ()

let pack_unpack_list () =
  let open Kx in
  pack_unpack "empty" list [||] ;
  pack_unpack "simple" (t1 (a bool)) true ;
  pack_unpack "simple2" (t2 (a int) (a float)) (0l, 0.) ;
  pack_unpack "vect"
    (t2 (v short) (v timestamp))
    ([|1; 2; 3|], ([|Ptime.epoch; Ptime.epoch|])) ;
  pack_unpack "vect guid"
    (t1 (v guid)) [|Uuidm.nil; Uuidm.nil|] ;
  pack_unpack "nested"
    (t1 (t1 (a bool))) true ;
  ()

let pack_unpack_dict () =
  let open Kx in
  pack_unpack "empty" (dict list list) ([||], [||]);
  ()

let pack_unpack_table () =
  let open Kx in
  pack_unpack "empty" (table (v sym) (v long)) ([|"a"|], [|1L|]);
  ()

let pack_unpack_conv () =
  let open Kx in
  pack_unpack "bool3" (t3 (a bool) (a bool) (a bool)) (true, false, true) ;
  pack_unpack "boolnested"
    (t3 (t3 (a short) (a short) (a short)) (a bool) (a bool))
    ((1, 2, 3), false, true) ;
  ()

let test_server () =
  let open Kx in
  let key = v sym in
  let values = t9
      (v sym) (v sym) (v sym)
      (v int) (v int) (v timestamp)
      (v timestamp) (v timestamp) list in
  let retwit = table key values in
  with_connection
    (Uri.make ~userinfo:"discovery:pass" ~host:"localhost" ~port:6001 ())
    ~f:begin fun fd ->
      k0_sync fd
        ".servers.SERVERS" retwit
    end |> function
  | Ok (Ok _a) -> ()
  | Ok (Error msg) -> failwith msg
  | Error msg -> failwith (Format.asprintf "%a" pp_connection_error msg)

let do_n_times n m f () =
  for i = 0 to n - 1 do
    f () ;
    if i mod m = 0 then Gc.compact ()
  done
let hu = do_n_times 100 10

let utilities () =
  check int "month1" 0 (int_of_month (2000, 1, 0)) ;
  check int "month2" 4 (int_of_month (2000, 5, 0)) ;
  check int "month3" 12 (int_of_month (2001, 1, 0)) ;
  check int "month4" 228 (int_of_month (2019, 1, 0)) ;
  check int "month5" 229 (int_of_month (2019, 2, 0)) ;
  for _ = 0 to 1000 do
    let i = Random.int 1000 in
    let j = int_of_month (month_of_int i) in
    check int "month" i j
  done

let tests_kx = [
  test_case "utilities" `Quick (utilities) ;
  test_case "atom" `Quick (hu pack_unpack_atom) ;
  test_case "vect" `Quick (hu pack_unpack_vect) ;
  test_case "list" `Quick (hu pack_unpack_list) ;
  test_case "dict" `Quick (hu pack_unpack_dict) ;
  test_case "table" `Quick (hu pack_unpack_table) ;
  test_case "conv" `Quick (hu pack_unpack_conv) ;
  test_case "server" `Quick test_server ;
]

(* let tests_kx_async = Alcotest_async.[
 * ] *)

let () =
  Kx.init () ;
  run "q" [
    "kx", tests_kx ;
    (* "kx-async", tests_kx_async ; *)
  ]
