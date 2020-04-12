open Kx
open Alcotest

let make_testable : type a. a w -> a testable =
 fun a -> testable (Kx.pp a) (fun x y -> Kx.equal a x a y)

let pack_unpack : type a. string -> a w -> a -> unit =
 fun name w a ->
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
        (Int32.of_int (Bigstringaf.length serialized))
        len;
      check tt name a v

let pack_unpack msg w v = test_case msg `Quick (fun () -> pack_unpack msg w v)

let pack_unpack_atom =
  Kx.
    [
      pack_unpack "bool" (a bool) true;
      pack_unpack "guid" (a guid) Uuidm.nil;
      pack_unpack "byte" (a byte) '\x00';
      pack_unpack "0h" (a short) 0;
      pack_unpack "0Nh" (a short) nh;
      pack_unpack "0Wh" (a short) wh;
      pack_unpack "-0Wh" (a short) minus_wh;
      pack_unpack "0i" (a int) 0l;
      pack_unpack "0Ni" (a int) ni;
      pack_unpack "0Wi" (a int) wi;
      pack_unpack "-0Wi" (a int) minus_wi;
      pack_unpack "0j" (a long) 0L;
      pack_unpack "0Nj" (a long) nj;
      pack_unpack "0Wj" (a long) wj;
      pack_unpack "-0Wj" (a long) minus_wj;
      pack_unpack "0e" (a real) 0.;
      pack_unpack "0Ne" (a real) nf;
      pack_unpack "0We" (a real) wf;
      pack_unpack "-0We" (a real) minus_wf;
      pack_unpack "0f" (a float) 0.;
      pack_unpack "0Nf" (a float) nf;
      pack_unpack "0Wf" (a float) wf;
      pack_unpack "-0Wf" (a float) minus_wf;
      pack_unpack "char" (a char) '\x00';
      pack_unpack "empty symbol" (a sym) "";
      pack_unpack "symbol" (a sym) "aiue";
      pack_unpack "timestamp" (a timestamp) Ptime.epoch;
      pack_unpack "timestamp_null" (a timestamp) Kx.np;
      pack_unpack "timestamp_older" (a timestamp)
        (Option.get (Ptime.of_date (2019, 01, 01)));
      pack_unpack "timestamp_2243" (a timestamp)
        (Option.get (Ptime.of_date (2243, 01, 01)));
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
        [| 0l; 1l; 2l; Int32.max_int; Int32.min_int |];
      pack_unpack "vect long" (v long)
        [| 0L; 1L; 2L; Int64.max_int; Int64.min_int |];
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
  let w2 =
    union
      [
        case
          (conv (fun a -> a) (fun _ -> invalid_arg "inj") (a Kx.int))
          (fun a -> Some a)
          (fun a -> a);
        case
          (conv (fun a -> a) (fun a -> a) (a Kx.int))
          (fun a -> Some a)
          (fun a -> a);
      ]
  in
  [
    pack_unpack "int case" w (Int 2);
    pack_unpack "float case" w (Float 4.);
    pack_unpack "broken int case" w2 0l;
  ]

let unpack_buggy () =
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

let test_sync hex w () =
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
    Bigstringaf.set_int64_le buf (i * 8) (Random.int64 Int64.max_int)
  done;
  try
    let buf' = compress buf in
    Printf.printf "compressed: %S\n" (to_string buf');
    let newlen = length buf' in
    let oldlen = length buf in
    Printf.printf "compress successful, ratio %g\n"
      (Int.to_float newlen /. Int.to_float oldlen);
    let uncompLen = Int32.to_int (get_int32_le buf' 8) in
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
  let testable = testable pp_print_date Stdlib.( = ) in
  List.iter
    (fun d ->
      let d' = date_of_int32 (int32_of_date d) in
      check testable (Format.asprintf "%a" pp_print_date d) d d')
    ds

let date () =
  date
    [
      (1985, 5, 20);
      (2019, 6, 10);
      (2019, 1, 1);
      (2019, 1, 2);
      (2000, 1, 1);
      (3000, 1, 1);
      (9999, 1, 1);
    ]

let timestamp ts =
  let testable = testable Ptime.pp Stdlib.( = ) in
  List.iter
    (fun ts ->
      let ts' = timestamp_of_int64 (int64_of_timestamp ts) in
      check testable (Format.asprintf "%a" Ptime.pp ts) ts ts')
    ts

let timestamp () = timestamp [ Ptime.v (100000, 0L) ]

let tests_utils =
  [
    test_case "compress" `Quick compress;
    test_case "utilities" `Quick utilities;
    test_case "date" `Quick date;
    test_case "timestamp" `Quick timestamp;
  ]

let tests_kx =
  [
    test_case "unpack_buggy" `Quick unpack_buggy;
    test_case "various" `Quick test_various;
    test_case "sorted dict atom" `Quick
      (test_sync
         (`Hex
           "01000000210000007f0b0102000000610062000600020000000200000003000000")
         Kx.(dict ~sorted:true (v ~attr:Sorted sym) (v int)));
    test_case "dict vector" `Quick
      (test_sync
         (`Hex
           "010000002d000000630b0002000000610062000000020000000600010000000200000006000100000003000000")
         Kx.(dict (v sym) (list (v int))));
    test_case "table" `Quick
      (test_sync
         (`Hex
           "010000002f0000006200630b0002000000610062000000020000000600010000000200000006000100000003000000")
         Kx.(table (list (v int))));
    test_case "sorted table" `Quick
      (test_sync
         (`Hex
           "010000002f0000006201630b0002000000610062000000020000000603010000000200000006000100000003000000")
         Kx.(table ~sorted:true (t2 (v ~attr:Parted int) (v int))));
    test_case "sorted table2" `Quick
      (test_sync
         (`Hex
           "010000002f0000006201630b0002000000610062000000020000000603010000000200000006000100000003000000")
         Kx.(table2 ~sorted:true int int));
    test_case "strange float" `Quick
      (test_sync (`Hex "0100000011000000f759f03b5dc8d78840") Kx.(a float));
    test_case "strange float vect" `Quick
      (test_sync (`Hex "010000001600000009000100000059f03b5dc8d78840")
         Kx.(v float));
    test_case "strange dict" `Quick
      (test_sync
         (`Hex
           "0100000028000000630b000100000073697a650000000100000009000100000059f03b5dc8d78840")
         Kx.(dict (v sym) (list (v float))));
    test_case "strange table" `Quick
      (test_sync
         (`Hex
           "010000002a0000006200630b000100000073697a650000000100000009000100000059f03b5dc8d78840")
         Kx.(table1 float));
    test_case "strange table2" `Quick
      (test_sync
         (`Hex
           "01000000430000006200630b000200000073697a65007472616465636f756e740000000200000009000100000059f03b5dc8d788400700010000003b22000000000000")
         Kx.(table2 float long));
    test_case "keyed_table" `Quick
      (test_sync
         (`Hex
           "0100000068000000636200630b000100000073796d000000010000000b00010000005842545553442e434250006200630b000200000073697a65007472616465636f756e740000000200000009000100000064cc5d4bc8d788400700010000003b22000000000000")
         Kx.(dict (table1 sym) (table2 float long)));
    test_case "keyed_table2" `Quick
      (test_sync
         (`Hex
           "0100000068000000636201630b000100000073796d000000010000000b00010000005842545553442e434250006200630b000200000073697a65007472616465636f756e740000000200000009000100000059f03b5dc8d788400700010000003b22000000000000")
         Kx.(dict (table1 ~sorted:true sym) (table2 float long)));
    test_case "keyed_table3" `Quick
      (test_sync
         (`Hex
           "0100000068000000636201630b000100000073796d000000010000000b00010000005842545553442e434250006200630b000200000073697a65007472616465636f756e740000000200000009000100000059f03b5dc8d788400700010000003b22000000000000")
         Kx.(dict (table1 ~sorted:true sym) (table2 float long)));
    test_case "keyed_table4" `Quick
      (test_sync
         (`Hex
           "0100000069000000636200630b000100000073796d000000010000000b0001000000414c474f5553442e434250006200630b000200000073697a65007472616465636f756e740000000200000009000100000000000000d41f1a410700010000005001000000000000")
         Kx.(dict (table1 sym) (table2 float long)));
    test_case "keyed_table5" `Quick
      (test_sync
         (`Hex
           "0100000069000000636200630b000100000073796d000000010000000b0001000000414c474f5553442e434250006200630b000200000073697a65007472616465636f756e740000000200000009000100000000000000d41f1a410700010000005001000000000000")
         Kx.(dict (table1 sym) (table2 float long)));
    test_case "keyed_table7" `Quick
      (test_sync
         (`Hex
           "01000000840000007f6201630b000100000073796d000000010000000b0102000000414c474f5553442e434250004241544554482e434250006200630b000200000073697a65007472616465636f756e740000000200000009000200000000000000d07e19410000000040f3d14007000200000045010000000000001900000000000000")
         Kx.(dict ~sorted:true (table1 ~sorted:true sym) (table2 float long)));
    test_case "keyed_table8" `Quick
      (test_sync
         (`Hex
           "0100000069000000636200630b000100000073796d000000010000000b0001000000414c474f5553442e434250006200630b000200000073697a65007472616465636f756e740000000200000009000100000000000000d07e19410700010000004501000000000000")
         Kx.(dict (table1 sym) (table2 float long)));
    test_case "keyed_table9" `Quick
      (test_sync
         (`Hex
           "01000000690000007f6201630b000100000073796d000000010000000b0001000000414c474f5553442e434250006200630b000200000073697a65007472616465636f756e740000000200000009000100000000000000d07e19410700010000004501000000000000")
         Kx.(dict ~sorted:true (table1 ~sorted:true sym) (table2 float long)));
    test_case "table10" `Quick
      (test_sync
         (`Hex
           "01000000380000006201630b000100000073796d000000010000000b0102000000414c474f5553442e434250004241544554482e43425000")
         Kx.(table1 ~sorted:true sym));
    test_case "trade0" `Quick
      (test_sync
         (`Hex
           "01000000e70000006200630b000800000074696d650073796d00696400677569640070726963650073697a65007369646500666c6167000000080000000c0002000000c0e8f390dd60db08c0e8f390dd60db080b00020000005842545553442e424d58005842545553442e424d580007000200000000000000000000800000000000000080020002000000caf7bfe2ecd3b4dece05bc17955ebdf9284ee8cb7a7608af97774ebdbac9bae70900020000000000000000a7b7400000000000a7b740090002000000000000000000f03f00000000000022400a000200000073730a00020000007070")
         Kx.(table8 timestamp sym long guid float float char char));
    test_case "trade1" `Quick
      (test_sync
         (`Hex
           "01000000240100006200630b000800000074696d650073796d00696400677569640070726963650073697a65007369646500666c6167000000080000000c000300000000645172de60db0800645172de60db0800645172de60db080b00030000005842545553442e424d58005842545553442e424d58005842545553442e424d580007000300000000000000000000800000000000000080000000000000008002000300000062f018c6fe2e41f1666567f3c8a0564d590a94a1f9bd54b1a77699565df2c9f8a64b4a4b714ab10d173e1258a2c51b470900030000000000000000a7b7400000000000a7b7400000000000a7b7400900030000000000000000faac4000000000002caf400000000000faa1400a00030000007373730a0003000000707070")
         Kx.(table8 timestamp sym long guid float float char char));
    test_case "trade2" `Quick
      (test_sync
         (`Hex
           "01000000aa0000006200630b000800000074696d650073796d00696400677569640070726963650073697a65007369646500666c6167000000080000000c0001000000c0e55b99de60db080b00010000005842545553442e424d580007000100000000000000000000800200010000000002d1f376bf68fc6c19319056ae7b420900010000000000000000a7b7400900010000000000000000003f400a0001000000730a000100000070")
         Kx.(table8 timestamp sym long guid float float char char));
    test_case "trade3" `Quick
      (test_sync
         (`Hex
           "01000000aa0000006200630b000800000074696d650073796d00696400677569640070726963650073697a65007369646500666c6167000000080000000c000100000080e397e5de60db080b00010000005842545553442e424d580007000100000000000000000000800200010000009cdd3336ac1ab33e9e9afe85b38c8cf80900010000000000000000a7b74009000100000000000000000036400a0001000000730a000100000070")
         Kx.(table8 timestamp sym long guid float float char char));
  ]

let () =
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
    ]
