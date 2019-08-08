open Kx
open Alcotest

let make_testable : type a. a w -> a testable = fun a ->
  testable (Kx.pp a) (fun x y -> Kx.equal a x a y)

let pack_unpack : type a. string -> a w -> a -> unit = fun name w a ->
  let tt = make_testable w in
  let hdr = Faraday.create 8 in
  let payload = Faraday.create 1024 in
  construct ~hdr ~payload w a ;
  let hdr_str = Faraday.serialize_to_string hdr in
  let serialized = hdr_str ^ Faraday.serialize_to_string payload in
  let serialized_hex = Hex.of_string serialized in
  (* Hex.hexdump serialized_hex ; *)
  Format.printf "%a@." Hex.pp serialized_hex ;
  match Angstrom.parse_string (destruct_exn w) serialized with
  | Error msg -> fail msg
  | Ok (hdr', v) ->
    let buf = Faraday.create 8 in
    write_hdr buf hdr' ;
    check string (name ^ "_header") hdr_str (Faraday.serialize_to_string buf) ;
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
  pack_unpack "lambda" (a lambda) ("", "{x+y}");
  pack_unpack "lambda_ctx" (a lambda) ("d", "{x+y}");
  ()

let pack_unpack_vect () =
  let open Kx in
  pack_unpack "vect bool" (v bool) [true; false] ;
  pack_unpack "vect guid" (v guid) [Uuidm.nil; Uuidm.nil] ;
  pack_unpack "vect byte" (s byte) "\x00\x01\x02" ;
  pack_unpack "vect short" (v short) [0;1;2] ;
  pack_unpack "vect int" (v int) [0l;1l;2l;Int32.max_int;Int32.min_int] ;
  pack_unpack "vect long" (v long) [0L;1L;2L;Int64.max_int;Int64.min_int] ;
  pack_unpack "vect real" (v real) [0.;1.;nan;infinity;neg_infinity] ;
  pack_unpack "vect float" (v float) [0.;1.;nan;infinity;neg_infinity] ;
  pack_unpack "vect char" (s char) "bleh" ;
  pack_unpack "vect symbol" (v sym) ["machin"; "truc"; "chouette"] ;
  pack_unpack "vect timestamp" (v timestamp) [Ptime.epoch; Ptime.epoch] ;
  pack_unpack "vect month" (v month) [2019, 1, 0 ; 2019, 2, 0] ;
  pack_unpack "vect date" (v date) [2019, 1, 1; 2019, 1, 2] ;
  pack_unpack "vect timespan" (v timespan) [] ;
  pack_unpack "vect minute" (v minute) [] ;
  pack_unpack "vect second" (v second) [] ;
  pack_unpack "vect time" (v time) [] ;
  pack_unpack "vect lambda" (v lambda) [("", "{x+y}"); ("d", "{x+y}")] ;
  ()

let pack_unpack_list () =
  let open Kx in
  pack_unpack "empty" (list (a bool)) [] ;
  pack_unpack "simple" (t1 (a bool)) true ;
  pack_unpack "simple2" (t2 (a int) (a float)) (0l, 0.) ;
  pack_unpack "simple3" (t3 (a int) (a float) (a sym)) (0l, 0., "a") ;
  pack_unpack "vect"
    (t2 (v short) (v timestamp))
    ([1; 2; 3], ([Ptime.epoch; Ptime.epoch])) ;
  pack_unpack "vect guid"
    (t1 (v guid)) [Uuidm.nil; Uuidm.nil] ;
  pack_unpack "nested"
    (t1 (t1 (a bool))) true ;
  pack_unpack "compound" (list (v short)) [[1;2]; [3;4]] ;
  pack_unpack "string list" (list (s char)) ["machin"; "truc"] ;
  pack_unpack "test" (t2 (a sym) (a bool)) ("auie", true) ;
  pack_unpack "t4" (t4 (a bool) (a bool) (a bool) (a bool)) (true, true, true, true) ;
  ()

let pack_unpack_dict () =
  let open Kx in
  pack_unpack "empty" (dict (list (a bool)) (list (a bool))) ([], []);
  pack_unpack "simple" (dict (v sym) (v long)) (["a";"b";"c"], [1L;2L;3L]);
  pack_unpack "compound"
    (dict
       (list (t2 (a bool) (a int)))
       (list (t2 (a bool) (a int))))
    ([true, 3l; false, 2l], [false, 1l; true, 2l]) ;
  pack_unpack "keyed table"
    (dict
       (table (v sym) (list (v short)))
       (table (v sym) (list (v short))))
    ((["id"], [[1; 2; 3]]), (["v"], [[4; 5; 6]])) ;
  ()

let pack_unpack_table () =
  let open Kx in
  pack_unpack "simple" (table (v sym) (v long)) (["a"], [1L]);
  pack_unpack "simple_2" (table (v sym) (t2 (v long) (v long))) (["a"], ([1L], [1L]));
  pack_unpack "symsym" (table (v sym) (t1 (v sym))) (["a"], ["truc"]);
  pack_unpack "symsym2" (table (v sym) (t2 (v timespan) (v sym))) (["a"], ([wn], ["truc"]));
  pack_unpack "symsym3" (table (v sym) (t2 (v timespan) (v long))) (["a"], ([wn], [0L]));
  pack_unpack "symsym3" (table (v sym) (t3 (v timespan) (v timespan) (v timespan))) (["a"], ([wn], [wn], [wn]));
  pack_unpack "symsym3" (table (v sym) (t3 (v timespan) (v long) (v sym))) (["a"], ([wn], [0L], ["truc"]));
  pack_unpack "simple_3" (table (v sym) (t3 (v timespan) (v sym) (v long))) (["a"], ([wn], ["truc"], [1L]));
  pack_unpack "trade0" begin
    let k = (v sym) in
    let v = t3 (v timespan) (v sym) (v long) in
    table k v
  end
    (["time"; "sym"; "side"; "size" ; "price"],
     ([wn], ["XBTUSD"], [1L])) ;
  pack_unpack "trade1" begin
    let k = (v sym) in
    let v = t4 (v timespan) (v sym) (v long) (v long) in
    table k v
  end
    (["time"; "sym"; "side"; "size" ; "price"],
     ([wn], ["XBTUSD"], [1L], [1L])) ;
  pack_unpack "trade1" begin
    let k = (v sym) in
    let v = t4 (v timespan) (v sym) (v char) (v long) in
    table k v
  end
    (["time"; "sym"; "side"; "size" ; "price"],
     ([wn], ["XBTUSD"], ['a'], [1L])) ;
  pack_unpack "trade" begin
    let k = (v sym) in
    let v = t5 (v timespan) (v sym) (v char) (v long) (v float) in
    table k v
  end
    (["time"; "sym"; "side"; "size" ; "price"],
     ([wn], ["XBTUSD"], ['b'], [1L], [1.])) ;
  ()

let pack_unpack_conv () =
  let open Kx in
  pack_unpack "bool3" (t3 (a bool) (a bool) (a bool)) (true, false, true) ;
  pack_unpack "boolnested"
    (t3 (t3 (a short) (a short) (a short)) (a bool) (a bool))
    ((1, 2, 3), false, true) ;
  ()

type union =
  | Int of int
  | Float of float

let pack_unpack_union () =
  let w = union [
      case (a Kx.short) (function Int i -> Some i | _ -> None) (fun i -> Int i) ;
      case (a Kx.float) (function Float i -> Some i | _ -> None) (fun i -> Float i) ;
    ] in
  pack_unpack "int case" w (Int 2) ;
  pack_unpack "float case" w (Float 4.)

let unpack_buggy () =
  let test = "\001\002\000\0004\000\000\000\245:/home/vb/code/TorQ/hdb/database2019.06.21\000" in
  match Angstrom.parse_string (destruct_exn (a sym)) test with
  | Error msg -> fail msg
  | Ok (_hdr, v) -> check string "buggy_one" ":/home/vb/code/TorQ/hdb/database2019.06.21" v

let test_server () =
  let open Core in
  let open Async in
  let open Kx_async in
  let t = create (t2 (s Kx.char) (a Kx.bool)) ("upd", false) in
  with_connection_async
    (Uri.make ~host:"localhost" ~port:5042 ()) ~f:begin fun _ p ->
    Pipe.write p t
  end >>= function
  | Error e ->
    failwithf "%s" (Format.asprintf "%a" Kx_async.pp_print_error e) ()
  | Ok () -> Deferred.unit

let utilities () =
  check int32 "month1" 0l (int32_of_month (2000, 1, 0)) ;
  check int32 "month2" 4l (int32_of_month (2000, 5, 0)) ;
  check int32 "month3" 12l (int32_of_month (2001, 1, 0)) ;
  check int32 "month4" 228l (int32_of_month (2019, 1, 0)) ;
  check int32 "month5" 229l (int32_of_month (2019, 2, 0)) ;
  for _ = 0 to 1000 do
    let i = Random.int32 1000l in
    let j = int32_of_month (month_of_int32 i) in
    check int32 "month" i j
  done

let date ds =
  let testable = testable pp_print_date Pervasives.(=) in
  List.iter begin fun d ->
    let d' = date_of_int32 (int32_of_date d) in
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
  test_case "union" `Quick pack_unpack_union ;
  test_case "unpack_buggy" `Quick unpack_buggy ;
]

let tests_kx_async = Alcotest_async.[
    test_case "server" `Quick test_server ;
  ]

let () =
  Logs.set_level ~all:true (Some Logs.Debug) ;
  run "q" [
    "kx", tests_kx ;
    (* "kx-async", tests_kx_async ; *)
  ]
