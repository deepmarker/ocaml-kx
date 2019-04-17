open Kx
open Alcotest

let t = testable Kx.pp Kx.equal

let pack_unpack name v =
  let vv = pack v in
  (* let vv_serialized = to_bigstring vv in
   * let vv_parsed = of_bigstring_exn vv_serialized in
   * let vv = unpack vv_parsed in *)
  let vv = unpack vv in
  check t name v vv

let pack_unpack_atom () =
  pack_unpack "bool" (Atom (Bool true)) ;
  pack_unpack "guid" (Atom (Guid Uuidm.nil)) ;
  pack_unpack "byte" (Atom (Byte 0)) ;
  pack_unpack "short" (Atom (Short 0)) ;
  pack_unpack "int" (Atom (Int 0l)) ;
  pack_unpack "long" (Atom (Long 0L)) ;
  pack_unpack "real" (Atom (Real 0.)) ;
  pack_unpack "float" (Atom (Float 0.)) ;
  pack_unpack "char" (Atom (Char '\x00')) ;
  pack_unpack "symbol" (Atom (Symbol "")) ;
  pack_unpack "timestamp" (Atom (Timestamp Ptime.epoch)) ;
  pack_unpack "month" (Atom (Month 0)) ;
  pack_unpack "date" (Atom (Date (2017, 10, 6))) ;
  pack_unpack "timespan" (Atom zero_timespan) ;
  pack_unpack "minute" (Atom (Minute ((0, 0, 0), 0))) ;
  pack_unpack "second" (Atom (Second ((0, 0, 0),0))) ;
  pack_unpack "time" (Atom zero_timespan) ;
  pack_unpack "datetime" (Atom (Datetime 0.)) ;
  ()

let pack_unpack_vect () =
  pack_unpack "vect bool" (VectArray.bool [|true; false|]) ;
  pack_unpack "vect guid" (VectArray.guid [|Uuidm.nil ; Uuidm.nil|]) ;
  pack_unpack "vect byte" (VectArray.byte (Bigstring.of_string "\x00\x01\x02")) ;
  pack_unpack "vect short" (VectArray.short [|0;1;2|]) ;
  pack_unpack "vect int" (VectArray.int [|0l;1l;2l;Int32.max_int;Int32.min_int|]) ;
  pack_unpack "vect long" (VectArray.long [|0L;1L;2L;Int64.max_int;Int64.min_int|]) ;
  pack_unpack "vect real" (VectArray.real [|0.;1.;nan;infinity;neg_infinity|]) ;
  pack_unpack "vect float" (VectArray.float [|0.;1.;nan;infinity;neg_infinity|]) ;
  pack_unpack "vect char" (VectArray.char "bleh") ;
  pack_unpack "vect symbol" (VectArray.symbol [|"machin"; "truc"; "chouette"|]) ;
  pack_unpack "vect timestamp" (VectArray.timestamp [|Ptime.epoch; Ptime.epoch|]) ;
  pack_unpack "vect month" (VectArray.month [|0;1;2|]) ;
  pack_unpack "vect date" (VectArray.date [|(2019, 1, 1);(2019, 1, 2)|]) ;
  (* pack_unpack "vect timespan" (Atom zero_timespan) ;
   * pack_unpack "vect minute" (Atom (Minute (0, 0))) ;
   * pack_unpack "vect second" (Atom (Second (0, 0, 0))) ;
   * pack_unpack "vect time" (Atom zero_timespan) ;
   * pack_unpack "vect datetime" (vector (float_vect (float64_arr [|0.;1.;nan;infinity;neg_infinity|]))) ; *)
  ()

let pack_unpack_list () =
  (* pack_unpack "empty" (Kx.List []) ; *)
  pack_unpack "simple" (Kx.List [Atom.bool false]) ;
  (* pack_unpack "simple" (Kx.List [Atom.int 0l; Atom.float 1.]) ;
   * pack_unpack "vect" (Kx.List [VectArray.short [|0;1;2|];
   *                              VectArray.timestamp [|Ptime.epoch; Ptime.epoch|]]) ;
   * pack_unpack "vect guid" (Kx.List [VectArray.guid [|Uuidm.nil; Uuidm.nil|]]) ; *)
  ()

let bindings () =
  check int "dj 0" 20000101 (dj 0) ;
  ()

let test_pack_unpack () =
  for i = 0 to 4000 do
    if i mod 5 = 0 then Gc.compact () ;
    (* pack_unpack_atom () ;
     * pack_unpack_vect () ; *)
    pack_unpack_list () ;
    ()
  done

let test_server () =
  Kx.with_connection
    (Uri.make ~userinfo:"discovery:pass" ~host:"localhost" ~port:6001 ())
    ~f:begin fun fd ->
    let k = Kx.kn_sync fd "`getservices" [|pack (Kx.vector (Vect.symbol ["tickerplant"]));
                                           Kx.kfalse|] in
    Kx.unpack k
  end |> function
  | Ok (Table (k, v)) ->
    check t "cols"       (Vector (Vect.symbol ["procname"; "proctype"; "hpup"; "attributes"])) k ;
    check t "vals" (List [Vector (Vect.symbol []);
                          Vector (Vect.symbol []);
                          Vector (Vect.symbol []);
                          List []]) v ;
    ()
  | Ok _ -> assert false
  | Error msg -> failwith (Format.asprintf "%a" pp_connection_error msg)

let tests_kx = [
  test_case "bindings" `Quick bindings ;
  test_case "pack_unpack" `Quick test_pack_unpack ;
  (* test_case "server" `Quick test_server ; *)
]

(* let tests_kx_async = Alcotest_async.[
 * ] *)

let () =
  Kx.initialize () ;
  run "q" [
    "kx", tests_kx ;
    (* "kx-async", tests_kx_async ; *)
  ]
