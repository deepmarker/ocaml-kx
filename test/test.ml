open Kx
open Alcotest

let make_testable : type a. a w -> a testable = fun a ->
  testable (Kx.pp a) (fun x y -> Kx.equal a x a y)

let pack_unpack : type a. string -> a w -> a -> unit = fun name w a ->
  let tt = make_testable w in
  let buf = Faraday.create 1024 in
  let hdr = construct buf w a in
  let serialized = string_of_hdr hdr ^ Faraday.serialize_to_string buf in
  Hex.hexdump (Hex.of_string serialized) ;
  match Angstrom.parse_string (destruct w) serialized with
  | Error msg -> fail msg
  | Ok (hdr', v) ->
    assert (hdr = hdr') ;
    check tt name a v

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
  pack_unpack "empty" (list (a bool)) [||] ;
  pack_unpack "simple" (t1 (a bool)) true ;
  pack_unpack "simple2" (t2 (a int) (a float)) (0l, 0.) ;
  pack_unpack "vect"
    (t2 (v short) (v timestamp))
    ([|1; 2; 3|], ([|Ptime.epoch; Ptime.epoch|])) ;
  pack_unpack "vect guid"
    (t1 (v guid)) [|Uuidm.nil; Uuidm.nil|] ;
  pack_unpack "nested"
    (t1 (t1 (a bool))) true ;
  pack_unpack "compound" (list (v short)) [|[|1;2|]; [|3;4|]|] ;
  pack_unpack "string list" (list (s char)) [|"machin"; "truc"|] ;
  ()

let pack_unpack_dict () =
  let open Kx in
  pack_unpack "empty" (dict (list (a bool)) (list (a bool))) ([||], [||]);
  pack_unpack "simple" (dict (v sym) (v long)) ([|"a";"b";"c"|], [|1L;2L;3L|]);
  pack_unpack "compound"
    (dict
       (list (t2 (a bool) (a int)))
       (list (t2 (a bool) (a int))))
    ([|true, 3l; false, 2l|], [|false, 1l; true, 2l|]);
  ()

let pack_unpack_table () =
  let open Kx in
  pack_unpack "simple" (table (v sym) (v long)) ([|"a"|], [|1L|]);
  ()

let pack_unpack_conv () =
  let open Kx in
  pack_unpack "bool3" (t3 (a bool) (a bool) (a bool)) (true, false, true) ;
  pack_unpack "boolnested"
    (t3 (t3 (a short) (a short) (a short)) (a bool) (a bool))
    ((1, 2, 3), false, true) ;
  ()

(* let test_server () =
 *   let open Kx in
 *   let key = v sym in
 *   let values = t9
 *       (v sym) (v sym) (v sym)
 *       (v int) (v int) (v timestamp)
 *       (v timestamp) (v timestamp) list in
 *   let retwit = table key values in
 *   with_connection
 *     (Uri.make ~userinfo:"discovery:pass" ~host:"localhost" ~port:6001 ())
 *     ~f:begin fun fd ->
 *       k0_sync fd
 *         ".servers.SERVERS" retwit
 *     end |> function
 *   | Ok (Ok _a) -> ()
 *   | Ok (Error msg) -> failwith msg
 *   | Error msg -> failwith (Format.asprintf "%a" pp_connection_error msg) *)

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

let date ds =
  let testable = testable pp_print_date Pervasives.(=) in
  List.iter begin fun d ->
    let d' = date_of_int (int_of_date d) in
    check testable (Format.asprintf "%a" pp_print_date d) d d'
  end ds

let date () =
  date [
    1985, 5, 20 ;
    2019, 6, 10 ;
    2019, 1, 1 ;
    2019, 1, 2 ;
    2000, 1, 1 ;
  ]

let tests_kx = [
  test_case "utilities" `Quick utilities ;
  test_case "date" `Quick date ;
  test_case "atom" `Quick pack_unpack_atom ;
  test_case "vect" `Quick pack_unpack_vect ;
  test_case "list" `Quick pack_unpack_list ;
  test_case "dict" `Quick pack_unpack_dict ;
  test_case "table" `Quick pack_unpack_table ;
  test_case "conv" `Quick pack_unpack_conv ;
  (* test_case "server" `Quick test_server ; *)
]

(* let tests_kx_async = Alcotest_async.[
 * ] *)

let () =
  run "q" [
    "kx", tests_kx ;
    (* "kx-async", tests_kx_async ; *)
  ]
