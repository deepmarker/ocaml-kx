open Kx
open Alcotest

let pp_ignore ppf (_:t) =
  Format.pp_print_string ppf "<t>"

let t = testable pp_ignore Pervasives.(=)

let pack_unpack v =
  let vv = unpack (pack v) in
  check t "" v vv

let pack_unpack_atom () =
  pack_unpack (Atom (Bool true)) ;
  pack_unpack (Atom (Guid Uuidm.nil)) ;
  pack_unpack (Atom (Byte 0)) ;
  pack_unpack (Atom (Short 0)) ;
  pack_unpack (Atom (Short 0)) ;
  pack_unpack (Atom (Int 0l)) ;
  pack_unpack (Atom (Long 0L)) ;
  pack_unpack (Atom (Real 0.)) ;
  pack_unpack (Atom (Float 0.)) ;
  pack_unpack (Atom (Char '\x00')) ;
  pack_unpack (Atom (Symbol "")) ;
  pack_unpack (Atom (Symbol "")) ;

  ()

let test_pack_unpack () =
  pack_unpack_atom ()

let trip = [
  test_case "pack_unpack" `Quick test_pack_unpack ;
]

let () =
  run "q" [
    "trip", trip
  ]
