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
  | None -> failwith "pack_unpack"
  | Some vv ->
    let vvv = construct w vv in
    check (testable Kx.pp Kx.equal) name v vvv

let pack_unpack_atom () =
  let open Kx_final in
  pack_unpack "bool" (atom bool) true ;
  pack_unpack "guid" (atom guid) Uuidm.nil ;
  pack_unpack "byte" (atom byte) '\x00' ;
  pack_unpack "short" (atom short) 0 ;
  pack_unpack "int" (atom int) 0l ;
  pack_unpack "long" (atom long) 0L ;
  pack_unpack "real" (atom real) 0. ;
  pack_unpack "float" (atom float) 0. ;
  pack_unpack "char" (atom char) '\x00' ;
  pack_unpack "symbol" (atom sym) "" ;
  pack_unpack "timestamp" (atom timestamp) Ptime.epoch ;
  pack_unpack "month" (atom month) (2018, 04, 0) ;
  pack_unpack "date" (atom date) (2017, 10, 6) ;
  pack_unpack "timespan" (atom timespan) { time = (0, 0, 0), 0 ; ns = 0 } ;
  pack_unpack "minute" (atom minute) ((0, 0, 0), 0) ;
  pack_unpack "second" (atom second) ((0, 0, 0), 0) ;
  pack_unpack "time" (atom time) { time = ((0, 0, 0), 0) ; ms = 0 };
  ()

let pack_unpack_vect () =
  let open Kx_final in
  pack_unpack "vect bool" (vect bool) [|true; false|] ;
  pack_unpack "vect guid" (vect guid) [|Uuidm.nil; Uuidm.nil|] ;
  pack_unpack "vect byte" (string byte) "\x00\x01\x02" ;
  pack_unpack "vect short" (vect short) [|0;1;2|] ;
  pack_unpack "vect int" (vect int) [|0l;1l;2l;Int32.max_int;Int32.min_int|] ;
  pack_unpack "vect long" (vect long) [|0L;1L;2L;Int64.max_int;Int64.min_int|] ;
  pack_unpack "vect real" (vect real) [|0.;1.;nan;infinity;neg_infinity|] ;
  pack_unpack "vect float" (vect float) [|0.;1.;nan;infinity;neg_infinity|] ;
  pack_unpack "vect char" (string char) "bleh" ;
  pack_unpack "vect symbol" (vect sym) [|"machin"; "truc"; "chouette"|] ;
  pack_unpack "vect timestamp" (vect timestamp) [|Ptime.epoch; Ptime.epoch|] ;
  pack_unpack "vect month" (vect month) [|2019, 1, 0 ; 2019, 2, 0|] ;
  pack_unpack "vect date" (vect date) [|(2019, 1, 1);(2019, 1, 2)|] ;
  (* pack_unpack "vect timespan" (Atom zero_timespan) ;
   * pack_unpack "vect minute" (Atom (Minute (0, 0))) ;
   * pack_unpack "vect second" (Atom (Second (0, 0, 0))) ;
   * pack_unpack "vect time" (Atom zero_timespan) ;
   * pack_unpack "vect datetime" (vector (float_vect (float64_arr [|0.;1.;nan;infinity;neg_infinity|]))) ; *)
  ()

let pack_unpack_list () =
  let open Kx_final in
  pack_unpack "empty" nil () ;
  pack_unpack "simple" (cons (atom bool) nil) (true, ()) ;
  pack_unpack "simple2"
    (cons (atom int) (cons (atom float) nil)) (0l, (0., ())) ;
  pack_unpack "vect"
    (cons (vect short) (cons (vect timestamp) nil))
    ([|1; 2; 3|], ([|Ptime.epoch; Ptime.epoch|], ())) ;
  pack_unpack "vect guid"
    (cons (vect guid) nil)
    ([|Uuidm.nil; Uuidm.nil|], ()) ;
  ()

(* let bindings () =
 *   check int "dj 0" 20000101 (dj 0) ;
 *   () *)

let test_pack_unpack () =
  for _ = 0 to 1000 do
    pack_unpack_atom () ;
    pack_unpack_vect () ;
    (* if i mod 5 = 0 then Gc.compact () ; *)
    pack_unpack_list () ;
    ()
  done

let test_server () =
  let open Kx in
  let k = vect sym in
  let v = cons (vect sym) (cons (vect sym) (cons (vect sym))) in
  let retwit = table k v in
  with_connection
    (Uri.make ~userinfo:"discovery:pass" ~host:"localhost" ~port:6001 ())
    ~f:begin fun fd ->
      kn_sync fd
        ".servers.SERVERS" retwit
        [|construct (vect sym) [|"tickerplant"|] ;
          construct (atom bool) false |]
    end |> function
  | Ok (Some _a) ->
    (* check t "cols"       (Vector (Vect.symbol ["procname"; "proctype"; "hpup"; "attributes"])) k ;
     * check t "vals" (List [Vector (Vect.symbol []);
     *                       Vector (Vect.symbol []);
     *                       Vector (Vect.symbol []);
     *                       List []]) v ; *)
    ()
  | Ok _ -> assert false
  | Error msg -> failwith (Format.asprintf "%a" pp_connection_error msg)

let tests_kx = [
  (* test_case "bindings" `Quick bindings ; *)
  test_case "pack_unpack" `Quick test_pack_unpack ;
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
