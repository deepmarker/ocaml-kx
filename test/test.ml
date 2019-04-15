open Kx
open Alcotest

let list_of_ba ?(f=fun a -> a) ba =
  let len = Bigarray.Array1.dim ba in
  let res = ref [] in
  for i = len - 1 downto 0 do
    res := f (Bigarray.Array1.unsafe_get ba i) :: !res
  done ;
  !res

let pp_sep ppf () = Format.fprintf ppf " "
let rec pp ppf = function
  | Atom (Char a) ->
    Format.pp_print_char ppf a
  | Atom (Long a) ->
    Format.fprintf ppf "%Ld" a
  | Atom (Date { y ; m ; d }) ->
    Format.fprintf ppf "%d%d%d" y m d
  | Atom _ ->
    Format.pp_print_string ppf "Atom <abstract>"
  | Vector v -> begin
    match get_vector guid v with
    | Some v ->
      Format.fprintf ppf "`guid$(%a)"
        (Format.pp_print_list ~pp_sep Uuidm.pp) (guids_of_arr v)
    | None ->
      match get_vector real v with
      | Some v ->
        Format.fprintf ppf "`real$(%a)"
          Format.(pp_print_list ~pp_sep pp_print_float) (list_of_ba v)
      | None ->
        match get_vector Kx.float v with
        | Some v ->
          Format.fprintf ppf "`float$(%a)"
            Format.(pp_print_list ~pp_sep pp_print_float) (list_of_ba v)
        | None ->
          match get_vector Kx.symbol v with
          | Some syms ->
            Format.fprintf ppf "`symbol$(%a)"
              Format.(pp_print_list ~pp_sep pp_print_string) syms
          | None ->
            Format.pp_print_string ppf "Vector <abstract>"
  end
  | List vs ->
    Format.fprintf ppf "(%a)" (Format.pp_print_list ~pp_sep pp) vs
  | Dict (k, v) ->
    Format.fprintf ppf "{ k:%a; v:%a }" pp k pp v
  | Table (k, v) ->
    Format.fprintf ppf "{| k:%a; v:%a |}" pp k pp v

let t = testable pp Kx.equal

let pack_unpack name v =
  let vv = unpack (pack v) in
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
  pack_unpack "date" (Atom (Date { y = 2017 ; m = 10 ; d = 26 })) ;
  pack_unpack "timespan" (Atom zero_timespan) ;
  pack_unpack "minute" (Atom (Minute (0, 0))) ;
  pack_unpack "second" (Atom (Second (0, 0, 0))) ;
  pack_unpack "time" (Atom zero_timespan) ;
  pack_unpack "datetime" (Atom (Datetime 0.)) ;
  ()

let pack_unpack_vect () =
  pack_unpack "vect bool" (vector (bool_vect (bool_arr [|true; false|]))) ;
  pack_unpack "vect guid" (vector (guid_vect (guid_arr [Uuidm.nil ; Uuidm.nil]))) ;
  pack_unpack "vect byte" (vector (byte_vect (uint8_arr [|0;1;2|]))) ;
  pack_unpack "vect short" (vector (short_vect (int16_arr [|0;1;2|]))) ;
  pack_unpack "vect int" (vector (int_vect (int32_arr [|0l;1l;2l;Int32.max_int;Int32.min_int|]))) ;
  pack_unpack "vect long" (vector (long_vect (int64_arr [|0L;1L;2L;Int64.max_int;Int64.min_int|]))) ;
  pack_unpack "vect real" (vector (real_vect (float32_arr [|0.;1.;nan;infinity;neg_infinity|]))) ;
  pack_unpack "vect float" (vector (float_vect (float64_arr [|0.;1.;nan;infinity;neg_infinity|]))) ;
  pack_unpack "vect char" (vector (char_vect (Bigstring.of_string "bleh"))) ;
  pack_unpack "vect symbol" (vector (symbol_vect ["machin"; "truc"; "chouette"])) ;
  pack_unpack "vect timestamp" (vector (timestamp_vect (timestamp_arr [Ptime.epoch; Ptime.epoch]))) ;
  pack_unpack "vect month" (vector (month_vect (int32_arr [|0l;1l;2l|]))) ;
  pack_unpack "vect date" (vector (date_vect (int32_arr [|0l;1l;2l|]))) ;
  (* pack_unpack "vect timespan" (Atom zero_timespan) ;
   * pack_unpack "vect minute" (Atom (Minute (0, 0))) ;
   * pack_unpack "vect second" (Atom (Second (0, 0, 0))) ;
   * pack_unpack "vect time" (Atom zero_timespan) ;
   * pack_unpack "vect datetime" (vector (float_vect (float64_arr [|0.;1.;nan;infinity;neg_infinity|]))) ; *)
  ()

let bindings () =
  check int "dj 0" 20000101 (dj 0) ;
  ()

let test_pack_unpack () =
  pack_unpack_atom () ;
  pack_unpack_vect () ;
  ()

let test_server () =
  Kx.with_connection
    (Uri.make ~userinfo:"discovery:pass" ~host:"localhost" ~port:6001 ())
    ~f:begin fun fd ->
    let k = Kx.kn_sync fd "`getservices" [|pack (Kx.vector (symbol_vect ["tickerplant"]));
                                           Kx.kfalse|] in
    Kx.unpack k
  end |> function
  | Ok (Table (k, v)) ->
    check t "cols" (Vector (symbol_vect ["procname"; "proctype"; "hpup"; "attributes"])) k ;
    check t "vals" (List [Vector (symbol_vect []);
                          Vector (symbol_vect []);
                          Vector (symbol_vect []);
                          List []]) v ;
    ()
  | Ok _ -> assert false
  | Error msg -> failwith (Format.asprintf "%a" pp_connection_error msg)

let tests_kx = [
  test_case "bindings" `Quick bindings ;
  test_case "pack_unpack" `Quick test_pack_unpack ;
  test_case "server" `Quick test_server ;
]

(* let tests_kx_async = Alcotest_async.[
 * ] *)

let () =
  run "q" [
    "kx", tests_kx ;
    (* "kx-async", tests_kx_async ; *)
  ]
