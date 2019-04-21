module Kx = Kx_final
open Kx
open Alcotest

let pack_unpack
  : type a. string -> a w -> a -> unit = fun name w a ->
  let v = construct w a in
  (* let vv_serialized = to_bigstring vv in
   * let vv_parsed = of_bigstring_exn vv_serialized in
   * let vv = unpack vv_parsed in *)
  match destruct w v with
  | Error msg -> failwith msg
  | Ok vv ->
    let vvv = construct w vv in
    check (testable Kx.pp Kx.equal) name v vvv

let pack_unpack_atom () =
  let open Kx_final in
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
  pack_unpack "month" (a month) (2018, 04, 0) ;
  pack_unpack "date" (a date) (2017, 10, 6) ;
  pack_unpack "timespan" (a timespan) { time = (0, 0, 0), 0 ; ns = 0 } ;
  pack_unpack "minute" (a minute) ((0, 0, 0), 0) ;
  pack_unpack "second" (a second) ((0, 0, 0), 0) ;
  pack_unpack "time" (a time) { time = ((0, 0, 0), 0) ; ms = 0 };
  ()

let pack_unpack_vect () =
  let open Kx_final in
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
  pack_unpack "vect date" (v date) [|(2019, 1, 1);(2019, 1, 2)|] ;
  (* pack_unpack "vect timespan" (Atom zero_timespan) ;
   * pack_unpack "vect minute" (Atom (Minute (0, 0))) ;
   * pack_unpack "vect second" (Atom (Second (0, 0, 0))) ;
   * pack_unpack "vect time" (Atom zero_timespan) ;
   * pack_unpack "vect datetime" (vector (float_vect (float64_arr [|0.;1.;nan;infinity;neg_infinity|]))) ; *)
  ()

let pack_unpack_list () =
  let open Kx in
  pack_unpack "empty" nil () ;
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
  pack_unpack "empty" (dict nil nil) ((), ());
  ()

let pack_unpack_table () =
  let open Kx in
  pack_unpack "empty" (table (v sym) (v long)) ([|"a"|], [|1L|]);
  ()

(* let bindings () =
 *   check int "dj 0" 20000101 (dj 0) ;
 *   () *)

let test_server () =
  let open Kx in
  let key = v sym in
  let values = t9
      (v sym) (v sym) (v sym)
      (v long) (v long) (v timestamp)
      (v timestamp) (v timestamp) (dict nil nil) in
  let retwit = table key values in
  with_connection
    (Uri.make ~userinfo:"discovery:pass" ~host:"localhost" ~port:6001 ())
    ~f:begin fun fd ->
      k0_sync fd
        ".servers.SERVERS" retwit
    end |> function
  | Ok (Ok _a) ->
    (* check t "cols"       (Vector (Vect.symbol ["procname"; "proctype"; "hpup"; "attributes"])) k ;
     * check t "vals" (List [Vector (Vect.symbol []);
     *                       Vector (Vect.symbol []);
     *                       Vector (Vect.symbol []);
     *                       List []]) v ; *)
    ()
  | Ok (Error msg) -> failwith msg
  | Error msg -> failwith (Format.asprintf "%a" pp_connection_error msg)

let do_n_times n m f () =
  for i = 0 to n - 1 do
    f () ;
    if i mod m = 0 then Gc.compact ()
  done
let hu = do_n_times 100 10

let tests_kx = [
  (* test_case "bindings" `Quick bindings ; *)
  test_case "atom" `Quick (hu pack_unpack_atom) ;
  test_case "vect" `Quick (hu pack_unpack_vect) ;
  test_case "list" `Quick (hu pack_unpack_list) ;
  test_case "dict" `Quick (hu pack_unpack_dict) ;
  test_case "table" `Quick (hu pack_unpack_table) ;
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
