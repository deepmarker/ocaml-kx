open Core
open Async
open Kx
open Alcotest_async

let make_testable : type a. a w -> a Alcotest.testable =
 fun a -> Alcotest.testable (Kx.pp a) (fun x y -> Kx.equal a x a y)

let pack_unpack : type a. string -> a w -> a -> unit =
 fun name w a ->
  let open Alcotest in
  let tt = make_testable w in
  let serialized = construct_bigstring ~comp:true ~typ:Async w a in
  let payload =
    Bigstringaf.(sub serialized ~off:8 ~len:(length serialized - 8))
  in
  Format.printf "%a@." Hex.pp (Hex.of_bigstring serialized);
  match
    ( Angstrom.parse_bigstring hdr serialized,
      Angstrom.parse_bigstring (destruct w) payload )
  with
  | Error msg, _ | _, Error msg -> fail msg
  | Ok { big_endian; len; _ }, Ok v ->
      check bool (name ^ "_big_endian") Sys.big_endian big_endian;
      check int32 (name ^ "_msglen")
        (Int32.of_int_exn (Bigstringaf.length serialized))
        len;
      check tt name a v

let pack_unpack msg w v =
  test_case_sync msg `Quick (fun () -> pack_unpack msg w v)

let pack_unpack_atom =
  Kx.
    [
      pack_unpack "bool" (a bool) true;
      pack_unpack "guid" (a guid) Uuidm.nil;
      pack_unpack "byte" (a byte) '\x00';
      pack_unpack "short" (a short) 0;
      pack_unpack "int" (a int) 0l;
      pack_unpack "long" (a long) 0L;
      pack_unpack "real" (a real) 0.;
      pack_unpack "float" (a float) 0.;
      pack_unpack "char" (a char) '\x00';
      pack_unpack "empty symbol" (a sym) "";
      pack_unpack "symbol" (a sym) "aiue";
      pack_unpack "timestamp" (a timestamp) Ptime.epoch;
      pack_unpack "timestamp_null" (a timestamp) Kx.np;
      pack_unpack "month" (a month) (2018, 4, 0);
      pack_unpack "month" (a month) (2019, 1, 0);
      pack_unpack "month" (a month) (2019, 2, 0);
      pack_unpack "date" (a date) (2017, 10, 6);
      pack_unpack "timespan" (a timespan) (Ptime.Span.of_int_s 0);
      pack_unpack "timespan_null" (a timespan) Kx.nn;
      pack_unpack "minute" (a minute) (Ptime.Span.of_int_s 0);
      pack_unpack "second" (a second) (Ptime.Span.of_int_s 0);
      pack_unpack "time" (a time) (Ptime.Span.of_int_s 0);
      pack_unpack "time_null" (a time) Kx.nt;
      pack_unpack "lambda" (a lambda) ("", "{x+y}");
      pack_unpack "lambda_ctx" (a lambda) ("d", "{x+y}");
      pack_unpack "unary" (a unaryprim) Unary.neg;
      pack_unpack "unary" (a unaryprim) Unary.sum;
      pack_unpack "op" (a operator) Op.plus;
      pack_unpack "over" (a over) Op.join;
    ]

let pack_unpack_vect =
  Kx.
    [
      pack_unpack "vect bool" (v bool) [| true; false |];
      pack_unpack "vect guid" (v guid) [| Uuidm.nil; Uuidm.nil |];
      pack_unpack "vect byte" (s byte) "\x00\x01\x02";
      pack_unpack "vect short" (v short) [| 0; 1; 2 |];
      pack_unpack "vect int" (v int)
        [| 0l; 1l; 2l; Int32.max_value; Int32.min_value |];
      pack_unpack "vect long" (v long)
        [| 0L; 1L; 2L; Int64.max_value; Int64.min_value |];
      pack_unpack "vect real" (v real)
        Float.[| 0.; 1.; nan; infinity; neg_infinity |];
      pack_unpack "vect float" (v float)
        Float.[| 0.; 1.; nan; infinity; neg_infinity |];
      pack_unpack "vect char" (s char) "bleh";
      pack_unpack "vect empty symbol" (v sym) [| ""; ""; "" |];
      pack_unpack "vect symbol" (v sym) [| "machin"; "truc"; "chouette" |];
      pack_unpack "vect timestamp" (v timestamp) [| Ptime.epoch; Ptime.epoch |];
      pack_unpack "vect timestamp_null" (v timestamp) [| Kx.np; Kx.np |];
      pack_unpack "vect month" (v month) [| (2019, 1, 0); (2019, 2, 0) |];
      pack_unpack "vect date" (v date) [| (2019, 1, 1); (2019, 1, 2) |];
      pack_unpack "vect timespan" (v timespan) [||];
      pack_unpack "vect minute" (v minute) [||];
      pack_unpack "vect second" (v second) [||];
      pack_unpack "vect time" (v time) [||];
      pack_unpack "vect lambda" (v lambda) [| ("", "{x+y}"); ("d", "{x+y}") |];
    ]

let pack_unpack_list =
  Kx.
    [
      pack_unpack "empty" (list (a bool)) [||];
      pack_unpack "simple" (t1 (a bool)) true;
      pack_unpack "simple2" (t2 (a int) (a float)) (0l, 0.);
      pack_unpack "simple3" (t3 (a int) (a float) (a sym)) (0l, 0., "a");
      pack_unpack "vect"
        (t2 (v short) (v timestamp))
        ([| 1; 2; 3 |], [| Ptime.epoch; Ptime.epoch |]);
      pack_unpack "vect guid" (t1 (v guid)) [| Uuidm.nil; Uuidm.nil |];
      pack_unpack "nested" (t1 (t1 (a bool))) true;
      pack_unpack "compound" (list (v short)) [| [| 1; 2 |]; [| 3; 4 |] |];
      pack_unpack "string list" (list (s char)) [| "machin"; "truc" |];
      pack_unpack "test" (t2 (a sym) (a bool)) ("auie", true);
      pack_unpack "t4"
        (t4 (a bool) (a bool) (a bool) (a bool))
        (true, true, true, true);
    ]

let pack_unpack_dict =
  Kx.
    [
      pack_unpack "empty" (dict (list (a bool)) (list (a bool))) ([||], [||]);
      pack_unpack "simple"
        (dict (v sym) (v long))
        ([| "a"; "b"; "c" |], [| 1L; 2L; 3L |]);
      pack_unpack "compound"
        (dict (list (t2 (a bool) (a int))) (list (t2 (a bool) (a int))))
        ([| (true, 3l); (false, 2l) |], [| (false, 1l); (true, 2l) |]);
      pack_unpack "sorted dict atom values"
        (dict ~sorted:true (v ~attr:Sorted sym) (v bool))
        ([| "a"; "b" |], [| true; false |]);
      pack_unpack "keyed table"
        (dict (table (list (v short))) (table (list (v short))))
        (([| "id" |], [| [| 1; 2; 3 |] |]), ([| "v" |], [| [| 4; 5; 6 |] |]));
      pack_unpack "keyed table2"
        (dict (table1 short) (table1 short))
        (([| "id" |], [| 1; 2; 3 |]), ([| "v" |], [| 4; 5; 6 |]));
      pack_unpack "keyed table3"
        (dict (table1 sym) (table2 float long))
        ( ([| "sym" |], [| "XBTUSD.CBP" |]),
          ([| "size"; "tradecount" |], ([| 794.9728 |], [| 8763L |])) );
    ]

let pack_unpack_table =
  Kx.
    [
      pack_unpack "simple_bool" (table1 bool) ([| "a" |], [| true |]);
      pack_unpack "simple_sorted_bool1" (table1 ~sorted:true bool)
        ([| "a" |], [| true |]);
      pack_unpack "simple_sorted_bool2"
        (table2 ~sorted:true bool bool)
        ([| "a"; "b" |], ([| true |], [| false |]));
      pack_unpack "simple_long" (table (t1 (v long))) ([| "a" |], [| 1L |]);
      pack_unpack "simple_2"
        (table (t2 (v long) (v long)))
        ([| "a"; "b" |], ([| 1L |], [| 1L |]));
      pack_unpack "symsym" (table (t1 (v sym))) ([| "a" |], [| "truc" |]);
      pack_unpack "symsym2" (table2 timespan sym)
        ([| "a"; "b" |], ([| wn |], [| "truc" |]));
      pack_unpack "symsym3"
        (table (t2 (v timespan) (v long)))
        ([| "a"; "b" |], ([| wn |], [| 0L |]));
      pack_unpack "symsym3"
        (table (t3 (v timespan) (v timespan) (v timespan)))
        ([| "a"; "b"; "c" |], ([| wn |], [| wn |], [| wn |]));
      pack_unpack "symsym3"
        (table (t3 (v timespan) (v long) (v sym)))
        ([| "a"; "b"; "c" |], ([| wn |], [| 0L |], [| "truc" |]));
      pack_unpack "simple_3" (table3 timespan sym long)
        ([| "a"; "b"; "c" |], ([| wn |], [| "truc" |], [| 1L |]));
      pack_unpack "trade0"
        (table (t3 (v timespan) (v sym) (v long)))
        ([| "time"; "sym"; "size" |], ([| wn |], [| "XBTUSD" |], [| 1L |]));
      pack_unpack "trade1"
        (table (t4 (v timespan) (v sym) (v long) (v long)))
        ( [| "time"; "sym"; "size"; "price" |],
          ([| wn |], [| "XBTUSD" |], [| 1L |], [| 1L |]) );
      pack_unpack "trade1"
        (table4 timespan sym char long)
        ( [| "time"; "sym"; "side"; "size" |],
          ([| wn |], [| "XBTUSD" |], [| 'a' |], [| 1L |]) );
      pack_unpack "trade"
        (table5 timespan sym char long float)
        ( [| "time"; "sym"; "side"; "size"; "price" |],
          ([| wn |], [| "XBTUSD" |], [| 'b' |], [| 1L |], [| 1. |]) );
    ]

let pack_unpack_conv =
  Kx.
    [
      pack_unpack "bool3" (t3 (a bool) (a bool) (a bool)) (true, false, true);
      pack_unpack "boolnested"
        (t3 (t3 (a short) (a short) (a short)) (a bool) (a bool))
        ((1, 2, 3), false, true);
    ]

type union = Int of int | Float of float

let pack_unpack_union =
  let w =
    union
      [
        case (a Kx.short)
          (function Int i -> Some i | _ -> None)
          (fun i -> Int i);
        case (a Kx.float)
          (function Float i -> Some i | _ -> None)
          (fun i -> Float i);
      ]
  in
  [ pack_unpack "int case" w (Int 2); pack_unpack "float case" w (Float 4.) ]

let unpack_buggy () =
  let open Alcotest in
  let test =
    Cstruct.of_string
      "\001\002\000\0004\000\000\000\245:/home/vb/code/TorQ/hdb/database2019.06.21\000"
  in
  let payload = Cstruct.shift test 8 in
  match
    ( Angstrom.parse_bigstring hdr (Cstruct.to_bigarray test),
      Angstrom.parse_bigstring (destruct (a sym)) (Cstruct.to_bigarray payload)
    )
  with
  | Error msg, _ | _, Error msg -> fail msg
  | Ok _hdr, Ok v ->
      check string "buggy_one" ":/home/vb/code/TorQ/hdb/database2019.06.21" v

let test_various () =
  let sym = "\245a\000" in
  match Angstrom.parse_string Kx.(destruct (a sym)) sym with
  | Ok _ -> ()
  | Error e -> failwith e

let test_server () =
  let t =
    Kx_async.create
      (t3 (a Kx.operator) (a Kx.long) (a Kx.long))
      (Kx.Op.plus, 1L, 1L)
  in
  let url = Uri.make ~host:"localhost" ~port:5042 () in
  Kx_async.with_connection ~url
    ~f:(fun { w; _ } -> Pipe.write w t >>= fun () -> Deferred.Or_error.ok_unit)
    ()
  >>= function
  | Error e -> Error.raise e
  | Ok () -> Deferred.unit

let test_headersless msg w () =
  let r =
    Pipe.create_reader ~close_on_exception:false (fun w -> Pipe.write w msg)
  in
  Reader.of_pipe (Info.of_string "") r >>= fun r ->
  Angstrom_async.parse Kx.(destruct w) r >>= function
  | Ok _v -> Deferred.unit
  | Error e -> failwith e

let test_async hex w () =
  let r =
    Pipe.create_reader ~close_on_exception:false (fun w ->
        Pipe.write w (Hex.to_string hex))
  in
  Reader.of_pipe (Info.of_string "") r >>= fun r ->
  Angstrom_async.parse Kx.hdr r >>= fun _hdr ->
  Angstrom_async.parse Kx.(destruct w) r >>= function
  | Ok _v -> Deferred.unit
  | Error e -> failwith e

let test_nonasync hex w () =
  let buf = Hex.to_bigstring hex in
  let buflen = Bigstringaf.length buf in
  let _hdr = Angstrom.parse_bigstring Kx.hdr buf in
  match
    Angstrom.parse_bigstring
      Kx.(destruct w)
      (Bigstringaf.sub buf ~off:8 ~len:(buflen - 8))
  with
  | Ok _v -> ()
  | Error e -> failwith e

let eq_bigstring a b =
  let open Bigstringaf in
  let len = length a in
  len = length b && Bigstringaf.memcmp a 0 b 0 len = 0

let pp_bigstring ppf a = Format.fprintf ppf "%a" Hex.pp (Hex.of_bigstring a)

let bigstring = Alcotest.testable pp_bigstring eq_bigstring

let compress n =
  let open Bigstringaf in
  let buf = Cstruct.create 256 in
  let buf = Cstruct.to_bigarray buf in
  set buf 0 '\x01';
  set buf 1 '\x00';
  set_int16_le buf 2 0;
  set_int32_le buf 4 256l;
  for i = n to 31 do
    Bigstringaf.set_int64_le buf (i * 8) (Random.int64 Int64.max_value)
  done;
  try
    let buf' = compress buf in
    printf "compressed: %S\n" (to_string buf');
    let newlen = length buf' in
    let oldlen = length buf in
    printf "compress successful, ratio %g\n"
      (Int.to_float newlen /. Int.to_float oldlen);
    let uncompLen = Int32.to_int_exn (get_int32_le buf' 8) in
    let buf'' = create uncompLen in
    uncompress buf'' buf';
    blit buf ~src_off:0 buf'' ~dst_off:0 ~len:8;
    Alcotest.check bigstring "compressed" buf buf'';
    ()
  with Exit -> ()

let compress () =
  for _ = 0 to 1000 do
    compress (1 + Random.int 30)
  done

let utilities () =
  let open Alcotest in
  check int32 "month1" 0l (int32_of_month (2000, 1, 0));
  check int32 "month2" 4l (int32_of_month (2000, 5, 0));
  check int32 "month3" 12l (int32_of_month (2001, 1, 0));
  check int32 "month4" 228l (int32_of_month (2019, 1, 0));
  check int32 "month5" 229l (int32_of_month (2019, 2, 0));
  for _ = 0 to 1000 do
    let i = Random.int32 1000l in
    let j = int32_of_month (month_of_int32 i) in
    check int32 "month" i j
  done

let date ds =
  let open Alcotest in
  let testable = testable pp_print_date Stdlib.( = ) in
  List.iter
    ~f:(fun d ->
      let d' = date_of_int32 (int32_of_date d) in
      check testable (Format.asprintf "%a" pp_print_date d) d d')
    ds

let date () =
  date
    [ (1985, 5, 20); (2019, 6, 10); (2019, 1, 1); (2019, 1, 2); (2000, 1, 1) ]

let tests_utils =
  [
    test_case_sync "compress" `Quick compress;
    test_case_sync "utilities" `Quick utilities;
    test_case_sync "date" `Quick date;
  ]

let tests_kx =
  [
    test_case_sync "unpack_buggy" `Quick unpack_buggy;
    test_case_sync "various" `Quick test_various;
    test_case_sync "sorted dict atom" `Quick
      (test_nonasync
         (`Hex
           "01000000210000007f0b0102000000610062000600020000000200000003000000")
         Kx.(dict ~sorted:true (v ~attr:Sorted sym) (v int)));
    test_case_sync "dict vector" `Quick
      (test_nonasync
         (`Hex
           "010000002d000000630b0002000000610062000000020000000600010000000200000006000100000003000000")
         Kx.(dict (v sym) (list (v int))));
    test_case_sync "table" `Quick
      (test_nonasync
         (`Hex
           "010000002f0000006200630b0002000000610062000000020000000600010000000200000006000100000003000000")
         Kx.(table (list (v int))));
    test_case_sync "sorted table" `Quick
      (test_nonasync
         (`Hex
           "010000002f0000006201630b0002000000610062000000020000000603010000000200000006000100000003000000")
         Kx.(table ~sorted:true (t2 (v ~attr:Parted int) (v int))));
    test_case_sync "sorted table2" `Quick
      (test_nonasync
         (`Hex
           "010000002f0000006201630b0002000000610062000000020000000603010000000200000006000100000003000000")
         Kx.(table2 ~sorted:true int int));
    test_case_sync "strange float" `Quick
      (test_nonasync (`Hex "0100000011000000f759f03b5dc8d78840") Kx.(a float));
    test_case_sync "strange float vect" `Quick
      (test_nonasync (`Hex "010000001600000009000100000059f03b5dc8d78840")
         Kx.(v float));
    test_case_sync "strange dict" `Quick
      (test_nonasync
         (`Hex
           "0100000028000000630b000100000073697a650000000100000009000100000059f03b5dc8d78840")
         Kx.(dict (v sym) (list (v float))));
    test_case_sync "strange table" `Quick
      (test_nonasync
         (`Hex
           "010000002a0000006200630b000100000073697a650000000100000009000100000059f03b5dc8d78840")
         Kx.(table1 float));
    test_case_sync "strange table2" `Quick
      (test_nonasync
         (`Hex
           "01000000430000006200630b000200000073697a65007472616465636f756e740000000200000009000100000059f03b5dc8d788400700010000003b22000000000000")
         Kx.(table2 float long));
    test_case_sync "keyed_table" `Quick
      (test_nonasync
         (`Hex
           "0100000068000000636200630b000100000073796d000000010000000b00010000005842545553442e434250006200630b000200000073697a65007472616465636f756e740000000200000009000100000064cc5d4bc8d788400700010000003b22000000000000")
         Kx.(dict (table1 sym) (table2 float long)));
    test_case_sync "keyed_table2" `Quick
      (test_nonasync
         (`Hex
           "0100000068000000636201630b000100000073796d000000010000000b00010000005842545553442e434250006200630b000200000073697a65007472616465636f756e740000000200000009000100000059f03b5dc8d788400700010000003b22000000000000")
         Kx.(dict (table1 ~sorted:true sym) (table2 float long)));
    test_case_sync "keyed_table3" `Quick
      (test_nonasync
         (`Hex
           "0100000068000000636201630b000100000073796d000000010000000b00010000005842545553442e434250006200630b000200000073697a65007472616465636f756e740000000200000009000100000059f03b5dc8d788400700010000003b22000000000000")
         Kx.(dict (table1 ~sorted:true sym) (table2 float long)));
    test_case_sync "keyed_table4" `Quick
      (test_nonasync
         (`Hex
           "0100000069000000636200630b000100000073796d000000010000000b0001000000414c474f5553442e434250006200630b000200000073697a65007472616465636f756e740000000200000009000100000000000000d41f1a410700010000005001000000000000")
         Kx.(dict (table1 sym) (table2 float long)));
    test_case_sync "keyed_table5" `Quick
      (test_nonasync
         (`Hex
           "0100000069000000636200630b000100000073796d000000010000000b0001000000414c474f5553442e434250006200630b000200000073697a65007472616465636f756e740000000200000009000100000000000000d41f1a410700010000005001000000000000")
         Kx.(dict (table1 sym) (table2 float long)));
    test_case_sync "keyed_table7" `Quick
      (test_nonasync
         (`Hex
           "01000000840000007f6201630b000100000073796d000000010000000b0102000000414c474f5553442e434250004241544554482e434250006200630b000200000073697a65007472616465636f756e740000000200000009000200000000000000d07e19410000000040f3d14007000200000045010000000000001900000000000000")
         Kx.(
           dict ~sorted:true
             (table1 ~attr2:Sorted ~sorted:true sym)
             (table2 float long)));
    test_case_sync "keyed_table8" `Quick
      (test_nonasync
         (`Hex
           "0100000069000000636200630b000100000073796d000000010000000b0001000000414c474f5553442e434250006200630b000200000073697a65007472616465636f756e740000000200000009000100000000000000d07e19410700010000004501000000000000")
         Kx.(dict (table1 sym) (table2 float long)));
    test_case_sync "keyed_table9" `Quick
      (test_nonasync
         (`Hex
           "01000000690000007f6201630b000100000073796d000000010000000b0001000000414c474f5553442e434250006200630b000200000073697a65007472616465636f756e740000000200000009000100000000000000d07e19410700010000004501000000000000")
         Kx.(dict ~sorted:true (table1 ~sorted:true sym) (table2 float long)));
    test_case_sync "table10" `Quick
      (test_nonasync
         (`Hex
           "01000000380000006201630b000100000073796d000000010000000b0102000000414c474f5553442e434250004241544554482e43425000")
         Kx.(table1 ~sorted:true ~attr2:Sorted sym));
  ]

let tests_kx_async =
  [
    test_case "vect symbol headerless" `Quick
      (test_headersless
         "\011\000\003\000\000\000countbysym\000hloc\000search\000"
         Kx.(v sym));
    test_case "atom symbol headerless" `Quick
      (test_headersless "\245a\000" Kx.(a sym));
    test_case "vect bool" `Quick
      (test_async (`Hex "0100000011000000010003000000010100") Kx.(v bool));
    test_case "vect char" `Quick
      (test_async (`Hex "01000000110000000a0003000000617569") Kx.(v char));
    test_case "vect byte" `Quick
      (test_async (`Hex "01000000100000000400020000001234") Kx.(v byte));
    test_case "vect short" `Quick
      (test_async (`Hex "0100000014000000050003000000010002000300")
         Kx.(v short));
    test_case "vect int" `Quick
      (test_async (`Hex "01000000160000000600020000000100000002000000")
         Kx.(v int));
    test_case "vect long" `Quick
      (test_async
         (`Hex "010000001e00000007000200000001000000000000000200000000000000")
         Kx.(v long));
    test_case "vect float" `Quick
      (test_async
         (`Hex "010000001e000000090002000000000000000000f03f0000000000000040")
         Kx.(v float));
    test_case "atom empty symbol" `Quick
      (test_async (`Hex "010000000a000000f500") Kx.(a sym));
    test_case "atom symbol" `Quick
      (test_async (`Hex "010000000b000000f56100") Kx.(a sym));
    test_case "vect symbol" `Quick
      (test_async (`Hex "01000000120000000b000200000061006200") Kx.(v sym));
    test_case "keyed_table" `Quick
      (test_async
         (`Hex
           "0100000068000000636200630b000100000073796d000000010000000b00010000005842545553442e434250006200630b000200000073697a65007472616465636f756e740000000200000009000100000064cc5d4bc8d788400700010000003b22000000000000")
         Kx.(dict (table1 sym) (table2 float long)));
    test_case "keyed_table2" `Quick
      (test_async
         (`Hex
           "0100000068000000636201630b000100000073796d000000010000000b00010000005842545553442e434250006200630b000200000073697a65007472616465636f756e740000000200000009000100000059f03b5dc8d788400700010000003b22000000000000")
         Kx.(dict (table1 ~sorted:true sym) (table2 float long)));
    (* test_case "server" `Quick test_server ; *)
  ]

let main () =
  run "kx"
    [
      ("utils", tests_utils);
      ("atom", pack_unpack_atom);
      ("vect", pack_unpack_vect);
      ("list", pack_unpack_list);
      ("dict", pack_unpack_dict);
      ("table", pack_unpack_table);
      ("conv", pack_unpack_conv);
      ("union", pack_unpack_union);
      ("kx", tests_kx);
      ("kx-async", tests_kx_async);
    ]

let () =
  don't_wait_for (main ());
  never_returns (Scheduler.go ())
