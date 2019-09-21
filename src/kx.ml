(*---------------------------------------------------------------------------
   Copyright (c) 2018 Vincent Bernardoff. All rights reserved.
   Distributed under the ISC license, see terms at the end of the file.
  ---------------------------------------------------------------------------*)

open Sexplib.Std

type time = { time : Ptime.time ; ms : int }
type timespan = { time : Ptime.time ; ns : int }

let nh = 0xffff_8000
let wh = 0x7fff

let ni = Int32.min_int
let wi = Int32.max_int

let nj = Int64.min_int
let wj = Int64.max_int

let nf = nan
let wf = infinity

let ptime_neginf =
  let open Ptime in
  let d, ps = Span.to_d_ps (to_span min) in
  match of_span (Span.unsafe_of_d_ps (d, Int64.succ ps)) with
  | None -> assert false
  | Some t -> t

let millenium =
  match Ptime.of_date (2000, 1, 1) with
  | None -> assert false
  | Some t -> t

let of_day_exn d =
  match Ptime.Span.of_d_ps (Int32.to_int d, 0L) with
  | None -> invalid_arg "Ptime.Span.of_d_ps"
  | Some t -> t

let add_span_exn t s =
  match Ptime.add_span t s with
  | None -> invalid_arg "Ptime.add_span"
  | Some t -> t

let date_of_int32 d =
  if d = ni then (0, 1, 1)
  else if d = wi then (9999, 12, 12)
  else if d = Int32.neg wi then (0, 1, 2)
  else Ptime.to_date (add_span_exn millenium (of_day_exn d))

let int32_of_date = function
  | 0, 1, 1 -> ni
  | 0, 1, 2 -> Int32.neg wi
  | d ->
    match Ptime.of_date d with
    | None -> invalid_arg "int32_of_date"
    | Some t ->
      Int32.of_int (fst Ptime.(Span.to_d_ps (diff t millenium)))

let int32_of_time { time = ((h, m, s), _) ; ms } =
  Int32.of_int ((h * 3600 + m * 60 + s) * 1_000 + ms)

let time_of_int32 time =
  let open Int32 in
  let ms = rem time 1_000l in
  let s = div time 1_000l in
  let hh = div s 3600l in
  let mm = rem (div s 60l) 60l in
  let ss = rem s 60l in
  { time = ((to_int hh, to_int mm, to_int ss), 0); ms = to_int ms }

let int64_of_timespan { time = ((h, m, s), _) ; ns } =
  let open Int64 in
  add
    (mul (of_int (h * 3600 + m * 60 + s)) 1_000_000_000L)
    (of_int ns)

let timespan_of_int64 time =
  let ns = Int64.(to_int (rem time 1_000_000_000L)) in
  let s = Int64.(to_int (div time 1_000_000_000L)) in
  let hh = s / 3600 in
  let mm = (s / 60) mod 60 in
  let ss = s mod 60 in
  { time = (hh, mm, ss), 0 ; ns }

let kx_epoch, kx_epoch_span =
  match Ptime.of_date (2000, 1, 1) with
  | None -> assert false
  | Some t -> t, Ptime.to_span t

let day_in_ns d =
  Int64.(mul (of_int (d * 24 * 3600)) 1_000_000_000L)

let int64_of_timestamp = function
  | ts when Ptime.(equal ts min) -> nj
  | ts when Ptime.(equal ts max) -> wj
  | ts when Ptime.(equal ts ptime_neginf) -> Int64.neg wj
  | ts ->
    let span_since_kxepoch =
      Ptime.(Span.sub (to_span ts) kx_epoch_span) in
    let d, ps = Ptime.Span.to_d_ps span_since_kxepoch in
    Int64.(add (day_in_ns d) (div ps 1_000L))

let timestamp_of_int64 = function
  | i when Int64.(equal i nj) -> Ptime.min
  | i when Int64.(equal i wj) -> Ptime.max
  | i when Int64.(equal i (neg wj)) -> ptime_neginf
  | nanos_since_kxepoch ->
    let one_day_in_ns = day_in_ns 1 in
    let days_since_kxepoch =
      Int64.(to_int (div nanos_since_kxepoch one_day_in_ns)) in
    let remaining_ps =
      Int64.(mul (rem nanos_since_kxepoch one_day_in_ns) 1_000L) in
    let span =
      Ptime.Span.v (days_since_kxepoch, remaining_ps) in
    match Ptime.add_span kx_epoch span with
    | None -> invalid_arg "timestamp_of_int64"
    | Some ts -> ts

let int32_of_month (y, m, _) =
  Int32.of_int ((y - 2000) * 12 + (pred m))

let month_of_int32 m =
  let open Int32 in
  let y = div m 12l in
  let rem_m = rem m 12l in
  to_int (add 2000l y), to_int (succ rem_m), 0

let int32_of_minute ((hh, mm, _), tz) =
  if tz = min_int then Int32.min_int
  else if tz = max_int then Int32.max_int
  else if tz = min_int + 1 then Int32.(succ min_int)
  else Int32.of_int (hh * 60 + mm)

let minute_of_int32 i =
  if i = ni then (0, 0, 0), min_int
  else if i = wi then (0, 0, 0), max_int
  else if i = Int32.neg wi then (0, 0, 0), succ min_int
  else Int32.(to_int (div i 60l), to_int (rem i 60l), 0), 0

let int32_of_second ((hh, mm, ss), tz) =
  if tz = min_int then Int32.min_int
  else if tz = max_int then Int32.max_int
  else if tz = min_int + 1 then Int32.(succ min_int)
  else Int32.of_int (hh * 3600 + mm * 60 + ss)

let second_of_int32 i =
  if i = ni then (0, 0, 0), min_int
  else if i = wi then (0, 0, 0), max_int
  else if i = Int32.neg wi then (0, 0, 0), succ min_int
  else
    let open Int32 in
    let hh = div i 3600l in
    let mm = rem (div i 60l) 60l in
    let ss = rem i 60l in
    (to_int hh, to_int mm, to_int ss), 0

let nn = timespan_of_int64 nj
let wn = timespan_of_int64 wj
let minus_wn = timespan_of_int64 (Int64.neg wj)

let nt = time_of_int32 ni
let wt = time_of_int32 wi
let minus_wt = time_of_int32 (Int32.neg wi)

let nm = month_of_int32 ni
let wm = month_of_int32 wi
let minus_wm = month_of_int32 (Int32.neg wi)

let nd = date_of_int32 ni
let wd = date_of_int32 wi
let minus_wd = date_of_int32 (Int32.neg wi)

let nu = minute_of_int32 ni
let wu = minute_of_int32 wi
let minus_wu = minute_of_int32 (Int32.neg wi)

let nv = second_of_int32 ni
let wv = second_of_int32 wi
let minus_wv = second_of_int32 (Int32.neg wi)

type _ typ =
  | Nil : unit typ
  | Boolean : bool typ
  | Guid : Uuidm.t typ
  | Byte : char typ
  | Short : int typ
  | Int : int32 typ
  | Long : int64 typ
  | Real : float typ
  | Float : float typ
  | Char : char typ
  | Symbol : string typ
  | Timestamp : Ptime.t typ
  | Month : Ptime.date typ
  | Date : Ptime.date typ
  | Timespan : timespan typ
  | Minute : Ptime.time typ
  | Second : Ptime.time typ
  | Time : time typ
  | Lambda : (string * string) typ

type (_, _) eq = Eq : ('a, 'a) eq

let eq_typ : type a b. a typ -> b typ -> (a, b) eq option = fun a b ->
  match a, b with
  | Boolean, Boolean -> Some Eq
  | Guid, Guid -> Some Eq
  | Byte, Byte -> Some Eq
  | Short, Short -> Some Eq
  | Int, Int -> Some Eq
  | Long, Long -> Some Eq
  | Real, Real -> Some Eq
  | Float, Float -> Some Eq
  | Char, Char -> Some Eq
  | Symbol, Symbol -> Some Eq
  | Timestamp, Timestamp -> Some Eq
  | Month, Month -> Some Eq
  | Date, Date -> Some Eq
  | Timespan, Timespan -> Some Eq
  | Minute, Minute -> Some Eq
  | Second, Second -> Some Eq
  | Time, Time -> Some Eq
  | Lambda, Lambda -> Some Eq
  | _ -> None

let eq_typ_val : type a b. a typ -> a -> b typ -> b -> (a, b) eq option = fun a x b y ->
  match a, b with
  | Boolean, Boolean when x = y -> Some Eq
  | Guid, Guid when Uuidm.equal x y -> Some Eq
  | Byte, Byte when x = y -> Some Eq
  | Short, Short when x = y -> Some Eq
  | Int, Int when Int32.equal x y -> Some Eq
  | Long, Long when Int64.equal x y -> Some Eq
  | Real, Real when Float.equal x y -> Some Eq
  | Float, Float when Float.equal x y -> Some Eq
  | Char, Char when x = y -> Some Eq
  | Symbol, Symbol when String.equal x y -> Some Eq
  | Timestamp, Timestamp when Ptime.equal x y -> Some Eq
  | Month, Month -> if x = y then Some Eq else None
  | Date, Date when x = y -> Some Eq
  | Timespan, Timespan when x = y -> Some Eq
  | Minute, Minute when x = y -> Some Eq
  | Second, Second when x = y -> Some Eq
  | Time, Time when x = y -> Some Eq
  | Lambda, Lambda when String.equal (fst x) (fst y) && String.equal (snd x) (snd y) -> Some Eq
  | _ -> None

type attribute =
  | Sorted
  | Unique
  | Parted
  | Grouped

let char_of_attribute = function
  | None -> '\x00'
  | Some Sorted -> '\x01'
  | Some Unique -> '\x02'
  | Some Parted -> '\x03'
  | Some Grouped -> '\x04'

let attribute attr =
  Angstrom.(char (char_of_attribute attr) >>| ignore)

type _ w =
  | Err : string w
  | Atom : 'a typ -> 'a w
  | Vect : 'a typ * attribute option -> 'a list w
  | String : char typ * attribute option -> string w
  | List : 'a w * attribute option -> 'a list w
  | Tup : 'a w * attribute option -> 'a w
  | Tups : 'a w * 'b w * attribute option -> ('a * 'b) w
  | Dict : 'a w * 'b w * bool -> ('a * 'b) w
  | Table : string list w * 'b w * bool -> (string list * 'b) w
  | Conv : ('a -> 'b) * ('b -> 'a) * 'b w -> 'a w
  | Union : 'a case list -> 'a w

and _ case =
  | Case : { encoding : 'a w ;
             proj : ('t -> 'a option) ;
             inj : ('a -> 't) } -> 't case

let case encoding proj inj = Case { encoding ; proj ; inj }
let union cases = Union cases

let rec is_list_type : type a. a w -> bool = function
  | Tup _ -> true
  | Tups _ -> true
  | List _ -> true
  | Vect _ -> true
  | Table _ -> true
  | Conv (_, _, w) -> is_list_type w
  | _ -> false

let parted : type a. a w -> a w = function
  | Tup (a, _) -> Tup (a, Some Parted)
  | Tups (a, b, _) -> Tups (a, b, Some Parted)
  | List (a, _) -> List (a, Some Parted)
  | Vect (a, _) -> Vect (a, Some Parted)
  | _ -> invalid_arg "parted"

let rec equal_w : type a b. a w -> b w -> bool = fun a b ->
  match a, b with
  | Atom a, Atom b -> eq_typ a b <> None
  | Vect (a, aa), Vect (b, ba) -> eq_typ a b <> None && aa = ba
  | String (a, aa), String (b, ba) -> eq_typ a b <> None && aa = ba
  | List (a, aa), List (b, ba) -> equal_w a b && aa = ba
  | Tup (a, aa), Tup (b, ba) -> equal_w a b && aa = ba
  | Tups (a, b, aa), Tups (c, d, ba) -> equal_w a c && equal_w b d && aa = ba
  | Dict (a, b, s1), Dict (c, d, s2) -> equal_w a c && equal_w b d && s1 = s2
  | Table (a, b, s1), Table (c, d, s2) -> equal_w a c && equal_w b d && s1 = s2
  | Conv (_, _, a), Conv (_, _, b) -> equal_w a b
  | Union a, Union b ->
    List.fold_left2 (fun a c1 c2 -> a && equal_case c1 c2) true a b
  | _ -> false

and equal_case : type a b. a case -> b case -> bool =
  fun (Case { encoding ; _ }) (Case { encoding = e2; _ }) ->
  equal_w encoding e2

let rec equal : type a b. a w -> a -> b w -> b -> bool = fun aw x bw y ->
  match aw, bw with
  | Atom a, Atom b -> eq_typ_val a x b y  <> None
  | Vect (a, aa), Vect (b, ba) ->
    aa = ba &&
    List.length x = List.length y &&
    List.fold_left2 (fun acc x y -> acc && eq_typ_val a x b y <> None) true x y
  | String _, String _ -> String.equal x y
  | List (a, aa), List (b, ba) ->
    aa = ba &&
    List.length x = List.length y &&
    List.fold_left2 (fun acc x y -> acc && equal a x b y) true x y
  | Tup (a, aa), Tup (b, ba) -> aa = ba && equal a x b y
  | Tups (a, b, aa), Tups (c, d, ba) ->
    let x1, x2 = x in
    let y1, y2 = y in
    aa = ba && equal a x1 c y1 && equal b x2 d y2
  | Dict (a, b, s1), Dict (c, d, s2) ->
    let x1, x2 = x in
    let y1, y2 = y in
    equal a x1 c y1 && equal b x2 d y2 && s1 = s2
  | Table (a, b, s1), Table (c, d, s2) ->
    let x1, x2 = x in
    let y1, y2 = y in
    equal a x1 c y1 && equal b x2 d y2 && s1 = s2
  | Conv (p1, _, a), Conv (p2, _, b) ->
    equal a (p1 x) b (p2 y)
  | Union c1, Union c2 ->
    equal_w aw bw &&
    List.fold_left2 begin fun a
      (Case { encoding; proj ; _ })
      (Case { encoding = e2; proj = proj2 ; _ }) ->
      a && match (proj x), (proj2 y) with
      | None, None -> true
      | Some a, Some b -> equal encoding a e2 b
      | _ -> false
    end true c1 c2
  | _ -> false

let nil       = Nil
let bool      = Boolean
let guid      = Guid
let byte      = Byte
let short     = Short
let int       = Int
let long      = Long
let real      = Real
let float     = Float
let char      = Char
let sym       = Symbol
let timestamp = Timestamp
let month     = Month
let date      = Date
let timespan  = Timespan
let minute    = Minute
let second    = Second
let time      = Time
let lambda    = Lambda

let conv project inject a =
  Conv (project, inject, a)

let err = Err
let a a = Atom a
let v ?attr a = Vect (a, attr)
let s ?attr a = String (a, attr)

let list ?attr a = List (a, attr)

let tup a attr = Tup (a, attr)
let tups a b attr = Tups (a, b, attr)

let t1 ?attr a = tup a attr
let t2 ?attr a b = tups (tup a attr) (tup b attr) attr
let t3 ?attr a b c =
  conv
    (fun (a, b, c) -> (a, (b, c)))
    (fun (a, (b, c)) -> (a, b, c))
    (tups (tup a attr) (t2 ?attr b c) attr)

let t4 ?attr a b c d =
  conv
    (fun (a, b, c, d) -> (a, (b, c, d)))
    (fun (a, (b, c, d)) -> (a, b, c, d))
    (tups (tup a attr) (t3 ?attr b c d) attr)

let t5 ?attr a b c d e =
  conv
    (fun (a, b, c, d, e) -> (a, (b, c, d, e)))
    (fun (a, (b, c, d, e)) -> (a, b, c, d, e))
    (tups (tup a attr) (t4 ?attr b c d e) attr)

let t6 ?attr a b c d e f =
  conv
    (fun (a, b, c, d, e, f) -> (a, (b, c, d, e, f)))
    (fun (a, (b, c, d, e, f)) -> (a, b, c, d, e, f))
    (tups (tup a attr) (t5 ?attr b c d e f) attr)

let t7 ?attr a b c d e f g =
  conv
    (fun (a, b, c, d, e, f, g) -> (a, (b, c, d, e, f, g)))
    (fun (a, (b, c, d, e, f, g)) -> (a, b, c, d, e, f, g))
    (tups (tup a attr) (t6 ?attr b c d e f g) attr)

let t8 ?attr a b c d e f g h =
  conv
    (fun (a, b, c, d, e, f, g, h) -> (a, (b, c, d, e, f, g, h)))
    (fun (a, (b, c, d, e, f, g, h)) -> (a, b, c, d, e, f, g, h))
    (tups (tup a attr) (t7 ?attr b c d e f g h) attr)

let t9 ?attr a b c d e f g h i =
  conv
    (fun (a, b, c, d, e, f, g, h, i) -> (a, (b, c, d, e, f, g, h, i)))
    (fun (a, (b, c, d, e, f, g, h, i)) -> (a, b, c, d, e, f, g, h, i))
    (tups (tup a attr) (t8 ?attr b c d e f g h i) attr)

let t10 ?attr a b c d e f g h i j =
  conv
    (fun (a, b, c, d, e, f, g, h, i, j) -> (a, (b, c, d, e, f, g, h, i, j)))
    (fun (a, (b, c, d, e, f, g, h, i, j)) -> (a, b, c, d, e, f, g, h, i, j))
    (tups (tup a attr) (t9 ?attr b c d e f g h i j) attr)

let merge_tups t1 t2 =
  let rec is_tup : type t. t w -> attribute option option = function
    | Tup (_, a) -> Some a
    | Tups (_, _, a)(* by construction *) -> Some a
    | Conv (_, _, t) -> is_tup t
    | _ -> None in
  match is_tup t1, is_tup t2 with
  | Some a, Some b when a = b -> Tups (t1, t2, a)
  | _ -> invalid_arg "merge_tups"

let dict ?(sorted=false) k v =
  if not (is_list_type k && is_list_type v) then
    invalid_arg "dict keys and values must be lists" ;
  Dict (k, v, sorted)

let table ?(sorted=false) vs =
  if not (is_list_type vs) then
    invalid_arg "table keys and values must be lists" ;
  Table (v sym, vs, sorted)

let table1 ?(sorted=false) v1 =
  let attr = if sorted then Some Parted else None in
  Table (v sym, t1 (v ?attr v1), sorted)
let table2 ?(sorted=false) v1 v2 =
  let attr = if sorted then Some Parted else None in
  Table (v sym, t2 (v ?attr v1) (v v2), sorted)
let table3 ?(sorted=false) v1 v2 v3 =
  let attr = if sorted then Some Parted else None in
  Table (v sym, t3 (v ?attr v1) (v v2) (v v3), sorted)
let table4 ?(sorted=false) v1 v2 v3 v4 =
  let attr = if sorted then Some Parted else None in
  Table (v sym, t4 (v ?attr v1) (v v2) (v v3) (v v4), sorted)
let table5 ?(sorted=false) v1 v2 v3 v4 v5 =
  let attr = if sorted then Some Parted else None in
  Table (v sym, t5 (v ?attr v1) (v v2) (v v3) (v v4) (v v5), sorted)

(* let string_of_chars a = String.init (Array.length a) (Array.get a) *)
let pp_print_month ppf (y, m, _) = Format.fprintf ppf "%d.%dm" y m
let pp_print_date ppf (y, m, d) =  Format.fprintf ppf "%d.%d.%d" y m d
let pp_print_timespan ppf { time = ((hh, mm, ss), _) ; ns } = Format.fprintf ppf "%d:%d:%d.%d" hh mm ss ns
let pp_print_minute ppf ((hh, mm, _), _) = Format.fprintf ppf "%d:%d" hh mm
let pp_print_second ppf ((hh, mm, ss), _) = Format.fprintf ppf "%d:%d:%d" hh mm ss
let pp_print_time ppf { time = ((hh, mm, ss), _) ; ms } = Format.fprintf ppf "%d:%d:%d.%d" hh mm ss ms
(* let pp_print_symbols ppf syms = Array.iter (fun sym -> Format.fprintf ppf "`%s" sym) syms *)

let pp_print_timestamp ppf = function
  | ts when ts = Ptime.min -> Format.pp_print_string ppf "0Np"
  | ts when ts = Ptime.max -> Format.pp_print_string ppf "0Wp"
  | ts when ts = ptime_neginf -> Format.pp_print_string ppf "-0Wp"
  | ts -> Format.fprintf ppf "%a" (Ptime.pp_rfc3339 ~frac_s:9 ()) ts

let pp_print_lambda ppf (ctx, lambda) = Format.pp_print_string ppf (ctx ^ lambda)

module type FE = module type of Faraday.BE

let rec construct_list :
  type a. (module FE) -> Faraday.t -> a w -> a -> int = fun e buf w a ->
  match w with
  | Tup (w, _) ->
    construct e buf w a ;
    1
  | Tups (hw, tw, _) ->
    let lenh = construct_list e buf hw (fst a) in
    let lent = construct_list e buf tw (snd a) in
    lenh + lent
  | Conv (project, _, w) -> construct_list e buf w (project a)
  | _ -> assert false

and construct : type a. (module FE) -> Faraday.t -> a w -> a -> unit = fun e buf w a ->
  let open Faraday in
  let module FE = (val e : FE) in
  match w with
  | Union cases ->
    let rec do_cases = function
      | [] -> invalid_arg "construct: union"
      | Case { encoding; proj; _ } :: rest ->
        match proj a with
        | Some t -> construct e buf encoding t
        | None -> do_cases rest
    in
    do_cases cases

  | Err ->
    write_char buf '\x80' ;
    write_string buf a ;
    write_char buf '\x00'

  | List (w', attr) ->
    write_char buf '\x00' ;
    write_char buf (char_of_attribute attr) ;
    FE.write_uint32 buf (Int32.of_int (List.length a)) ;
    List.iter (construct e buf w') a

  | Tup (ww, attr) ->
    write_char buf '\x00' ;
    write_char buf (char_of_attribute attr) ;
    FE.write_uint32 buf 1l ;
    construct e buf ww a

  | Tups (_, _, attr) ->
    let buf' = create 13 in
    let len = construct_list e buf' w a in
    write_char buf '\x00' ;
    write_char buf (char_of_attribute attr) ;
    FE.write_uint32 buf (Int32.of_int len) ;
    (schedule_bigstring buf (serialize_to_bigstring buf'))

  | Dict (k, v, sorted) ->
    let x, y = a in
    write_char buf (if sorted then '\x7f' else '\x63') ;
    construct e buf k x ;
    construct e buf v y

  | Table (k, v, sorted) ->
    write_char buf '\x62' ;
    write_char buf (if sorted then '\x01' else '\x00') ;
    (* Apply parted attr here or not?? *)
    construct e buf (Dict (k, (if sorted then (parted v) else v), sorted)) a

  | Conv (project, _, ww) -> construct e buf ww (project a)

  | Atom Nil ->
    write_char buf '\x65' ;
    write_char buf '\x00'

  | Atom Boolean ->
    write_char buf '\xff' ;
    begin match a with
      | false -> write_char buf '\x00'
      | true -> write_char buf '\x01'
    end

  | Atom Guid ->
    write_char buf '\xfe' ;
    write_string buf (Uuidm.to_bytes a)

  | Atom Byte ->
    write_char buf '\xfc' ;
    write_char buf a

  | Atom Short ->
    write_char buf '\xfb' ;
    FE.write_uint16 buf a

  | Atom Int ->
    write_char buf '\xfa' ;
    FE.write_uint32 buf a ;

  | Atom Long ->
    write_char buf '\xf9' ;
    FE.write_uint64 buf a

  | Atom Real ->
    write_char buf '\xf8' ;
    FE.write_float buf a

  | Atom Float ->
    write_char buf '\xf7' ;
    FE.write_double buf a

  | Atom Char ->
    write_char buf '\xf6' ;
    write_char buf a

  | Atom Symbol ->
    write_char buf '\xf5' ;
    write_string buf a ;
    write_char buf '\x00'

  | Atom Timestamp ->
    write_char buf '\xf4' ;
    FE.write_uint64 buf (int64_of_timestamp a) ;

  | Atom Month ->
    write_char buf '\xf3' ;
    FE.write_uint32 buf (int32_of_month a)

  | Atom Date ->
    write_char buf '\xf2' ;
    FE.write_uint32 buf (int32_of_date a)

  | Atom Timespan ->
    write_char buf '\xf0' ;
    FE.write_uint64 buf (int64_of_timespan a)

  | Atom Minute ->
    write_char buf '\xef' ;
    FE.write_uint32 buf (int32_of_minute a)

  | Atom Second ->
    write_char buf '\xee' ;
    FE.write_uint32 buf (int32_of_second a)

  | Atom Time ->
    write_char buf '\xed' ;
    FE.write_uint32 buf (int32_of_time a)

  | Atom Lambda ->
    write_char buf '\x64' ;
    write_string buf (fst a) ;
    write_char buf '\x00' ;
    construct e buf (String (Char, None)) (snd a)

  | Vect (Lambda, attr) ->
    begin match attr with
    | None
    | Some Grouped -> ()
    | _ -> invalid_arg "lambda cannot have attr except grouped"
    end ;
    write_char buf '\x00' ;
    write_char buf (char_of_attribute attr) ;
    FE.write_uint32 buf (Int32.of_int (List.length a)) ;
    List.iter (fun lam -> construct e buf (Atom Lambda) lam) a

  | Vect (Nil, _) -> invalid_arg "nil vect is not allowed"

  | Vect (Boolean, attr) ->
    write_char buf '\x01' ;
    write_char buf (char_of_attribute attr) ;
    FE.write_uint32 buf (Int32.of_int (List.length a)) ;
    List.iter begin function
      | false -> write_char buf '\x00'
      | true -> write_char buf '\x01'
    end a

  | String (Byte, attr) ->
    write_char buf '\x04' ;
    write_char buf (char_of_attribute attr) ;
    FE.write_uint32 buf (Int32.of_int (String.length a)) ;
    write_string buf a

  | Vect (Byte, attr) ->
    write_char buf '\x04' ;
    write_char buf (char_of_attribute attr) ;
    FE.write_uint32 buf (Int32.of_int (List.length a)) ;
    List.iter (write_char buf) a

  | String (Char, attr) ->
    write_char buf '\x0a' ;
    write_char buf (char_of_attribute attr) ;
    FE.write_uint32 buf (Int32.of_int (String.length a)) ;
    write_string buf a

  | Vect (Char, attr) ->
    write_char buf '\x0a' ;
    write_char buf (char_of_attribute attr) ;
    FE.write_uint32 buf (Int32.of_int (List.length a)) ;
    List.iter (write_char buf) a

  | Vect (Short, attr) ->
    write_char buf '\x05' ;
    write_char buf (char_of_attribute attr) ;
    FE.write_uint32 buf (Int32.of_int (List.length a)) ;
    List.iter (FE.write_uint16 buf) a

  | Vect (Int, attr) ->
    write_char buf '\x06' ;
    write_char buf (char_of_attribute attr) ;
    FE.write_uint32 buf (Int32.of_int (List.length a)) ;
    List.iter (FE.write_uint32 buf) a

  | Vect (Long, attr) ->
    write_char buf '\x07' ;
    write_char buf (char_of_attribute attr) ;
    FE.write_uint32 buf (Int32.of_int (List.length a)) ;
    List.iter (FE.write_uint64 buf) a

  | Vect (Real, attr) ->
    write_char buf '\x08' ;
    write_char buf (char_of_attribute attr) ;
    FE.write_uint32 buf (Int32.of_int (List.length a)) ;
    List.iter (FE.write_float buf) a

  | Vect (Float, attr) ->
    write_char buf '\x09' ;
    write_char buf (char_of_attribute attr) ;
    FE.write_uint32 buf (Int32.of_int (List.length a)) ;
    List.iter (FE.write_double buf) a

  | Vect (Symbol, attr) ->
    write_char buf '\x0b' ;
    write_char buf (char_of_attribute attr) ;
    FE.write_uint32 buf (Int32.of_int (List.length a)) ;
    List.iter begin fun s ->
      write_string buf s ;
      write_char buf '\x00'
    end a

  | Vect (Guid, attr) ->
    write_char buf '\x02' ;
    write_char buf (char_of_attribute attr) ;
    FE.write_uint32 buf (Int32.of_int (List.length a)) ;
    List.iter begin fun s ->
      write_string buf (Uuidm.to_bytes s)
    end a

  | Vect (Timestamp, attr) ->
    write_char buf '\x0c' ;
    write_char buf (char_of_attribute attr) ;
    FE.write_uint32 buf (Int32.of_int (List.length a)) ;
    List.iter begin fun ts ->
      FE.write_uint64 buf (int64_of_timestamp ts)
    end a

  | Vect (Month, attr) ->
    write_char buf '\x0d' ;
    write_char buf (char_of_attribute attr) ;
    FE.write_uint32 buf (Int32.of_int (List.length a)) ;
    List.iter begin fun m ->
      FE.write_uint32 buf (int32_of_month m)
    end a

  | Vect (Date, attr) ->
    write_char buf '\x0e' ;
    write_char buf (char_of_attribute attr) ;
    FE.write_uint32 buf (Int32.of_int (List.length a)) ;
    List.iter begin fun m ->
      FE.write_uint32 buf (int32_of_date m)
    end a

  | Vect (Timespan, attr) ->
    write_char buf '\x10' ;
    write_char buf (char_of_attribute attr) ;
    FE.write_uint32 buf (Int32.of_int (List.length a)) ;
    List.iter begin fun a ->
      FE.write_uint64 buf (int64_of_timespan a)
    end a

  | Vect (Minute, attr) ->
    write_char buf '\x11' ;
    write_char buf (char_of_attribute attr) ;
    FE.write_uint32 buf (Int32.of_int (List.length a)) ;
    List.iter begin fun m ->
      FE.write_uint32 buf (int32_of_minute m)
    end a

  | Vect (Second, attr) ->
    write_char buf '\x12' ;
    write_char buf (char_of_attribute attr) ;
    FE.write_uint32 buf (Int32.of_int (List.length a)) ;
    List.iter begin fun m ->
      FE.write_uint32 buf (int32_of_second m)
    end a

  | Vect (Time, attr) ->
    write_char buf '\x13' ;
    write_char buf (char_of_attribute attr) ;
    FE.write_uint32 buf (Int32.of_int (List.length a)) ;
    List.iter begin fun a ->
      FE.write_uint32 buf (int32_of_time a)
    end a

  | String _ -> assert false

let char_of_msgtyp = function
  | `Async -> '\x00'
  | `Sync -> '\x01'
  | `Response -> '\x02'

type hdr = {
  big_endian: bool ;
  typ: [`Async | `Sync | `Response] ;
  compressed: bool ;
  len: int32 ;
} [@@deriving sexp]

let pp_print_hdr ppf t =
  Format.fprintf ppf "%a" Sexplib.Sexp.pp (sexp_of_hdr t)

let set_int32 ~big_endian =
  Bigstringaf.(if big_endian then set_int32_be else set_int32_le)
let ppincr x = incr x; !x
let incrpp x = let v = !x in incr x; v
let set_int8 t p i = Bigstringaf.set t p (Char.chr (0xff land i))
let get_int8 t p = Char.code (Bigstringaf.get t p)

let compress ?(big_endian=Sys.big_endian) uncompressed =
  let uncompLen = Bigstringaf.length uncompressed in
  let compLen = uncompLen / 2 in
  let compressed = Bigstringaf.create compLen in
  let i = ref 0 in
  let g = ref false in
  let f = ref 0 in
  let h0 = ref 0 in
  let h = ref 0 in
  let c = ref 12 in
  let d = ref !c in
  let p = ref 0 in
  let r = ref 0 in
  let s0 = ref 0 in
  let s = ref 8 in
  let a = Array.make 256 0 in
  Bigstringaf.blit uncompressed ~src_off:0 compressed ~dst_off:0 ~len:4 ;
  set_int8 compressed 2 1 ;
  set_int32 ~big_endian compressed 8 (Int32.of_int uncompLen) ;
  while !s < uncompLen do
    if 0 = !i then begin
      if !d > compLen - 17 then raise Exit ;
      i := 1;
      set_int8 compressed !c !f ;
      c := incrpp d ;
      f := 0
    end ;
    let clause2 () =
      h := get_int8 uncompressed !s lxor get_int8 uncompressed (succ !s) ;
      p := Array.get a !h ;
      !p in
    g := (!s > uncompLen - 3) || (0 = clause2 ()) ||
         (0 <> get_int8 uncompressed !s lxor get_int8 uncompressed !p) ;
    if (0 < !s0) then begin
      Array.set a !h0 !s0 ;
      s0 := 0
    end ;
    if !g then begin
      h0 := !h ;
      s0 := !s ;
      set_int8 compressed (incrpp d) (get_int8 uncompressed (incrpp s))
    end else begin
      Array.set a !h !s ;
      f := !f lor !i ;
      p := !p + 2 ;
      s := !s + 2 ;
      r := !s ;
      let q = min (!s + 255) uncompLen in
      while (get_int8 uncompressed !p =
             get_int8 uncompressed !s && ppincr s < q) do incr p done ;
      set_int8 compressed (incrpp d) !h ;
      set_int8 compressed (incrpp d) (!s - !r)
    end ;
    i := (!i * 2) mod 256
  done ;
  set_int8 compressed !c !f ;
  set_int32 ~big_endian compressed 4 (Int32.of_int !d) ;
  Bigstringaf.sub compressed ~off:0 ~len:!d

let construct ?(comp=false) ?(big_endian=Sys.big_endian) ~typ ?(buf=Faraday.create 4096) w x =
  let module FE =
    (val Faraday.(if big_endian then (module BE : FE) else (module LE : FE))) in
  construct (module FE) buf w x ;
  let len = Faraday.pending_bytes buf in
  let uncompressed = Bigstringaf.create (8 + len) in
  Bigstringaf.set uncompressed 0 (if big_endian then '\x00' else '\x01') ;
  Bigstringaf.set uncompressed 1 (char_of_msgtyp typ) ;
  Bigstringaf.set_int16_be uncompressed 2 0 ;
  set_int32 ~big_endian uncompressed 4 (Int32.of_int (8 + len)) ;
  let _ = Faraday.serialize buf begin fun iovecs ->
      let dst_off =
        List.fold_left begin fun dst_off { Faraday.buffer ; off ; len } ->
          Bigstringaf.blit buffer ~src_off:off uncompressed ~dst_off ~len ;
          dst_off+len
        end 8 iovecs in
      `Ok (dst_off-8)
    end in
  if comp && len > 2000 then
    try compress ~big_endian uncompressed with Exit -> uncompressed
  else uncompressed

let msgtyp_of_int = function
  | 0 -> `Async
  | 1 -> `Sync
  | 2 -> `Response
  | _ -> invalid_arg "msgtyp_of_int"

open Angstrom

let msgtyp = any_uint8 >>| msgtyp_of_int

let uint8_flag =
  any_uint8 >>| function
  | 0 -> false
  | 1 -> true
  | _ -> invalid_arg "uint8_flag"

module type ENDIAN = module type of BE

let getmod = function
  | true -> (module BE : ENDIAN)
  | false -> (module LE : ENDIAN)

let hdr =
  uint8_flag >>= fun is_little_endian ->
  let big_endian = not is_little_endian in
  msgtyp >>= fun typ ->
  uint8_flag >>= fun compressed ->
  any_uint8 >>= fun _ ->
  let module M = (val getmod big_endian) in
  M.any_int32 >>| fun len ->
  { big_endian ; compressed ; typ ; len }

let nil_atom =
  char '\x65' *>
  skip (fun c -> c = '\x00')

let bool_atom =
  char '\xff' *>
  any_uint8 >>| fun i ->
  i <> 0

let guid_encoding =
  take 16 >>| fun guid ->
  match Uuidm.of_bytes guid with
  | None -> invalid_arg "guid"
  | Some v -> v

let guid_atom =
  char '\xfe' *> guid_encoding

let byte_atom =
  char '\xfc' *> any_char

let short_encoding endianness =
  let module M = (val getmod endianness) in
  M.any_int16

let short_atom endianness =
  char '\xfb' *>
  short_encoding endianness

let int_encoding endianness =
  let module M = (val getmod endianness) in
  M.any_int32

let int_atom endianness =
  char '\xfa' *>
  int_encoding endianness

let long_encoding endianness =
  let module M = (val getmod endianness) in
  M.any_int64

let long_atom endianness =
  char '\xf9' *>
  long_encoding endianness

let real_encoding endianness =
  let module M = (val getmod endianness) in
  M.any_float

let real_atom endianness =
  char '\xf8' *>
  real_encoding endianness

let float_encoding endianness =
  let module M = (val getmod endianness) in
  M.any_double

let float_atom endianness =
  char '\xf7' *> float_encoding endianness

let char_atom =
  char '\xf6' *> any_char

let symbol_encoding =
  take_while (fun c -> c <> '\x00') <* char '\x00'

let symbol_atom =
  char '\xf5' *> symbol_encoding

let err_atom =
  char '\x80' *> symbol_encoding

let timestamp_encoding endianness =
  let module M = (val getmod endianness) in
  M.any_int64 >>| timestamp_of_int64

let timestamp_atom endianness =
  char '\xf4' *> timestamp_encoding endianness

let month_encoding endianness =
  let module M = (val getmod endianness) in
  M.any_int32 >>| month_of_int32

let month_atom endianness =
  char '\xf3' *> month_encoding endianness

let date_encoding endianness =
  let module M = (val getmod endianness) in
  M.any_int32 >>| date_of_int32

let date_atom endianness =
  char '\xf2' *> date_encoding endianness

let timespan_encoding endianness =
  let module M = (val getmod endianness) in
  M.any_int64 >>| timespan_of_int64

let timespan_atom endianness =
  char '\xf0' *> timespan_encoding endianness

let minute_encoding endianness =
  let module M = (val getmod endianness) in
  M.any_int32 >>| minute_of_int32

let minute_atom endianness =
  char '\xef' *> minute_encoding endianness

let second_encoding endianness =
  let module M = (val getmod endianness) in
  M.any_int32 >>| second_of_int32

let second_atom endianness =
  char '\xee' *> second_encoding endianness

let time_encoding endianness =
  let module M = (val getmod endianness) in
  M.any_int32 >>| time_of_int32

let time_atom endianness =
  char '\xed' *> time_encoding endianness

let length big_endian =
  let module M = (val getmod big_endian) in
  M.any_int32 >>| Int32.to_int

let bool_vect endianness attr =
  char '\x01' *>
  attribute attr >>= fun () ->
  length endianness >>= fun len ->
  take len >>| fun str ->
  List.init len (fun i -> str.[i] <> '\x00')

let guid_vect endianness attr =
  char '\x02' *>
  attribute attr >>= fun () ->
  length endianness >>= fun len ->
  count len guid_encoding

let char_array len =
  take len >>| fun str ->
  List.init len (fun i -> str.[i])

let byte_vect endianness attr =
  char '\x04' *>
  attribute attr >>= fun () ->
  length endianness >>= fun len ->
  char_array len

let bytestring_vect endianness attr =
  char '\x04' *>
  attribute attr >>= fun () ->
  length endianness >>= fun len ->
  take len

let short_vect endianness attr =
  char '\x05' *>
  attribute attr >>= fun () ->
  length endianness >>= fun len ->
  count len (short_encoding endianness)

let int_vect endianness attr =
  char '\x06' *>
  attribute attr >>= fun () ->
  length endianness >>= fun len ->
  count len (int_encoding endianness)

let long_vect endianness attr =
  char '\x07' *>
  attribute attr >>= fun () ->
  length endianness >>= fun len ->
  count len (long_encoding endianness)

let real_vect endianness attr =
  char '\x08' *>
  attribute attr >>= fun () ->
  length endianness >>= fun len ->
  count len (real_encoding endianness)

let float_vect endianness attr =
  char '\x09' *>
  attribute attr >>= fun () ->
  length endianness >>= fun len ->
  count len (float_encoding endianness)

let char_vect endianness attr =
  char '\x0a' *>
  attribute attr >>= fun () ->
  length endianness >>= fun len ->
  char_array len

let string_vect endianness attr =
  char '\x0a' *>
  attribute attr >>= fun () ->
  length endianness >>= fun len ->
  take len

let symbol_vect endianness attr =
  char '\x0b' *>
  attribute attr >>= fun () ->
  length endianness >>= fun len ->
  count len symbol_encoding

let timestamp_vect endianness attr =
  char '\x0c' *>
  attribute attr >>= fun () ->
  length endianness >>= fun len ->
  count len (timestamp_encoding endianness)

let month_vect endianness attr =
  char '\x0d' *>
  attribute attr >>= fun () ->
  length endianness >>= fun len ->
  count len (month_encoding endianness)

let date_vect endianness attr =
  char '\x0e' *>
  attribute attr >>= fun () ->
  length endianness >>= fun len ->
  count len (date_encoding endianness)

let timespan_vect endianness attr =
  char '\x10' *>
  attribute attr >>= fun () ->
  length endianness >>= fun len ->
  count len (timespan_encoding endianness)

let minute_vect endianness attr =
  char '\x11' *>
  attribute attr >>= fun () ->
  length endianness >>= fun len ->
  count len (minute_encoding endianness)

let second_vect endianness attr =
  char '\x12' *>
  attribute attr >>= fun () ->
  length endianness >>= fun len ->
  count len (second_encoding endianness)

let time_vect endianness attr =
  char '\x13' *>
  attribute attr >>= fun () ->
  length endianness >>= fun len ->
  count len (time_encoding endianness)

let lambda_atom endianness =
  char '\x64' *> symbol_encoding >>= fun sym ->
  string_vect endianness None >>| fun lam ->
  (sym, lam)

let general_list big_endian attr elt =
  char '\x00' *>
  attribute attr >>= fun () ->
  length big_endian >>= fun len ->
  count len elt

let stream n p f =
  if n < 0 then invalid_arg "stream: n < 0";
  let rec loop = function
    | 0 -> return ()
    | n ->
      lift f p >>= fun () ->
      loop (n - 1) in
  loop n

let general_list_stream endianness attr elt f =
  char '\x00' *>
  attribute attr >>= fun () ->
  length endianness >>= fun len ->
  stream len elt f

let lambda_vect endianness attr =
  general_list endianness attr (lambda_atom endianness)

let uncompress msg compressed =
  let n = ref 0 in
  let r = ref 0 in
  let f = ref 0 in
  let s = ref 8 in
  let p = ref !s in
  let i = ref 0 in
  let d = ref 12 in
  let aa = Array.make 256 0 in
  let msglen = Bigstringaf.length msg in
  while !s < msglen do
    if !i = 0 then begin
      f := get_int8 compressed (incrpp d) ;
      i := 1
    end ;
    if (!f land !i <> 0) then begin
      r := Array.get aa (get_int8 compressed (incrpp d)) ;
      set_int8 msg (incrpp s) (get_int8 msg (incrpp r)) ;
      set_int8 msg (incrpp s) (get_int8 msg (incrpp r)) ;
      n := get_int8 compressed (incrpp d) ;
      for m = 0 to !n - 1 do
        let rm = get_int8 msg (!r + m) in
        set_int8 msg (!s + m) rm
      done ;
    end else begin
      set_int8 msg (incrpp s) (get_int8 compressed (incrpp d)) ;
    end ;
    while !p < pred !s do
      let i1 = get_int8 msg !p in
      let i2 = get_int8 msg (succ !p) in
      Array.set aa (i1 lxor i2) (incrpp p)
    done ;
    if !f land !i <> 0 then begin
      s := !s + !n ;
      p := !s
    end ;
    i := (!i * 2) mod (0xffff+1) ;
    if !i = 256 then i := 0 ;
  done

let rec destruct_list :
  type a. bool -> a w -> a Angstrom.t = fun big_endian w ->
  match w with
  | Tup (ww, _) -> destruct ~big_endian ww
  | Tups (h, t, _) ->
    destruct_list big_endian h >>= fun a ->
    destruct_list big_endian t >>| fun b ->
    (a, b)
  | Conv (_, f, ww) -> lift f (destruct_list big_endian ww)
  | _ -> assert false

and destruct :
  type a. ?big_endian:bool -> a w -> a Angstrom.t =
  fun ?(big_endian=Sys.big_endian) w ->
  match w with
  | Union cases ->
    choice ~failure_msg:"union: no more choices"
      (List.map begin fun (Case { encoding ; inj ; _ }) ->
          lift inj (destruct ~big_endian encoding)
        end cases)
  | Err -> err_atom
  | List (w, attr) -> general_list big_endian attr (destruct ~big_endian w)
  | Conv (_, inject, w) -> destruct ~big_endian w >>| inject
  | Tup (w, attr) -> general_list big_endian attr (destruct ~big_endian w) >>| List.hd
  | Tups (_, _, attr) as t ->
    char '\x00' *>
    attribute attr >>= fun () ->
    length big_endian >>= fun _len ->
    destruct_list big_endian t

  | Dict (kw, vw, sorted) ->
    char (if sorted then '\x7f' else '\x63') *>
    destruct ~big_endian kw >>= fun k ->
    destruct ~big_endian vw >>| fun v ->
    k, v

  | Table (kw, vw, sorted) ->
    char '\x62' *>
    char (if sorted then '\x01' else '\x00') *>
    destruct ~big_endian (Dict (kw, vw, false))

  | Atom Nil -> nil_atom
  | Atom Boolean -> bool_atom
  | Atom Guid -> guid_atom
  | Atom Byte -> byte_atom
  | Atom Short -> short_atom big_endian
  | Atom Int -> int_atom big_endian
  | Atom Long -> long_atom big_endian
  | Atom Real -> real_atom big_endian
  | Atom Float -> float_atom big_endian
  | Atom Char -> char_atom
  | Atom Symbol -> symbol_atom
  | Atom Timestamp -> timestamp_atom big_endian
  | Atom Month -> month_atom big_endian
  | Atom Date -> date_atom big_endian
  | Atom Timespan -> timespan_atom big_endian
  | Atom Minute -> minute_atom big_endian
  | Atom Second -> second_atom big_endian
  | Atom Time -> time_atom big_endian
  | Atom Lambda -> lambda_atom big_endian

  | Vect (Nil, _) -> invalid_arg "nil vect is not allowed"
  | Vect (Boolean, attr) -> bool_vect big_endian attr
  | Vect (Guid, attr) -> guid_vect big_endian attr
  | Vect (Byte, attr) -> byte_vect big_endian attr
  | Vect (Short, attr) -> short_vect big_endian attr
  | Vect (Int, attr)  -> int_vect big_endian attr
  | Vect (Long, attr) -> long_vect big_endian attr
  | Vect (Real, attr) -> real_vect big_endian attr
  | Vect (Float, attr) -> float_vect big_endian attr
  | Vect (Char, attr) -> char_vect big_endian attr
  | Vect (Symbol, attr) -> symbol_vect big_endian attr
  | Vect (Timestamp, attr) -> timestamp_vect big_endian attr
  | Vect (Month, attr) -> month_vect big_endian attr
  | Vect (Date, attr) -> date_vect big_endian attr
  | Vect (Timespan, attr) -> timespan_vect big_endian attr
  | Vect (Minute, attr) -> minute_vect big_endian attr
  | Vect (Second, attr) -> second_vect big_endian attr
  | Vect (Time, attr) -> time_vect big_endian attr
  | Vect (Lambda, attr) -> lambda_vect big_endian attr

  | String (Byte, attr) -> bytestring_vect big_endian attr
  | String (Char, attr) -> string_vect big_endian attr
  | String _ -> invalid_arg "destruct: unsupported string type"

let destruct_stream :
  ?big_endian:bool -> 'a list w -> ('a -> unit) -> unit Angstrom.t =
  fun ?(big_endian=Sys.big_endian) w f ->
  match w with
  | List (w, attr) ->
    general_list_stream big_endian attr (destruct ~big_endian w) f
  | _ -> invalid_arg "destruct_stream: not a general list"

let destruct_exn ?big_endian v =
  choice ~failure_msg:"destruct_exn" [
    (destruct ?big_endian v) ;
    (destruct ?big_endian err >>| fun msg -> failwith msg) ;
  ]

let destruct ?big_endian v =
  choice ~failure_msg:"destruct" [
    (destruct ?big_endian v >>| fun v -> Ok v) ;
    (destruct ?big_endian err >>| fun msg -> Error msg) ;
  ]

let pp_print_short ppf i =
  if i = nh then Format.pp_print_string ppf "0Nh" else Format.pp_print_int ppf i
let pp_print_int ppf i =
  if i = ni then Format.pp_print_string ppf "0Ni" else Format.fprintf ppf "%ld" i
let pp_print_long ppf i =
  if i = nj then Format.pp_print_string ppf "0Nj" else Format.fprintf ppf "%Ld" i

let pp_print_real ppf i =
  match Float.classify_float i with
  | FP_nan -> Format.pp_print_string ppf "0Ne"
  | FP_infinite when i > 0. -> Format.pp_print_string ppf "0We"
  | FP_infinite -> Format.pp_print_string ppf "0We"
  | _ -> Format.fprintf ppf "%g" i

let pp_print_float ppf i =
  match Float.classify_float i with
  | FP_nan -> Format.pp_print_string ppf "0Nf"
  | FP_infinite when i > 0. -> Format.pp_print_string ppf "0Wf"
  | FP_infinite -> Format.pp_print_string ppf "0Wf"
  | _ -> Format.fprintf ppf "%g" i

let rec pp_print_list :
  type a. Format.formatter -> a w -> a -> unit = fun ppf w v ->
  match w with
  | Tup (ww, _) -> pp ww ppf v
  | Tups (h, t, _) ->
    pp_print_list ppf h (fst v) ;
    Format.pp_print_char ppf ' ' ;
    pp_print_list ppf t (snd v)
  | Conv (project, _, w) ->
    pp_print_list ppf w (project v)
  | _ -> assert false

and pp :
  type a. a w -> Format.formatter -> a -> unit = fun w ppf v ->
  let pp_sep ppf () = Format.pp_print_char ppf ' ' in
  let pp_sep_empty ppf () = Format.pp_print_string ppf "" in
  match w with
  | Atom Nil -> Format.pp_print_string ppf "(::)"
  | List (w, _) -> Format.(pp_print_list ~pp_sep (pp w) ppf v)
  | Conv (project, _, w) -> pp w ppf (project v)
  | Tup (w, _) -> pp w ppf v
  | Tups _ -> pp_print_list ppf w v
  | Dict (kw, vw, _sorted) ->
    Format.fprintf ppf "%a!%a" (pp kw) (fst v) (pp vw) (snd v)
  | Table (kw, vw, _sorted) ->
    Format.fprintf ppf "+%a!%a" (pp kw) (fst v) (pp vw) (snd v)
  | Atom Boolean -> Format.pp_print_bool ppf v
  | Atom Guid -> Uuidm.pp ppf v
  | Atom Byte -> Format.fprintf ppf "X%c" v
  | Atom Short -> pp_print_short ppf v
  | Atom Int -> pp_print_int ppf v
  | Atom Long -> pp_print_long ppf v
  | Atom Real -> pp_print_real ppf v
  | Atom Float -> pp_print_float ppf v
  | Atom Char -> Format.pp_print_char ppf v
  | Atom Symbol -> Format.fprintf ppf "`%s" v
  | Atom Timestamp -> pp_print_timestamp ppf v
  | Atom Month -> pp_print_month ppf v
  | Atom Date -> pp_print_date ppf v
  | Atom Timespan -> pp_print_timespan ppf v
  | Atom Minute -> pp_print_minute ppf v
  | Atom Second -> pp_print_second ppf v
  | Atom Time -> pp_print_time ppf v
  | Atom Lambda -> pp_print_lambda ppf v

  | Vect (Nil, _) -> invalid_arg "nil vect is not allowed"
  | Vect (Boolean, _) -> Format.pp_print_list ~pp_sep Format.pp_print_bool ppf v
  | Vect (Guid, _) -> Format.pp_print_list ~pp_sep Uuidm.pp ppf v
  | Vect (Byte, _) -> Format.pp_print_list ~pp_sep (fun ppf -> Format.fprintf ppf "X%c") ppf v
  | Vect (Short, _) ->Format.pp_print_list ~pp_sep pp_print_short ppf v
  | Vect (Int, _)  -> Format.pp_print_list ~pp_sep pp_print_int ppf v
  | Vect (Long, _) -> Format.pp_print_list ~pp_sep pp_print_long ppf v
  | Vect (Real, _) -> Format.pp_print_list ~pp_sep pp_print_real ppf v
  | Vect (Float, _) -> Format.pp_print_list ~pp_sep pp_print_float ppf v
  | Vect (Char, _) -> Format.pp_print_string ppf (String.init (List.length v) (List.nth v))
  | Vect (Symbol, _) -> Format.pp_print_list ~pp_sep:pp_sep_empty (fun ppf -> Format.fprintf ppf "`%s") ppf v
  | Vect (Timestamp, _) -> Format.pp_print_list ~pp_sep pp_print_timestamp ppf v
  | Vect (Month, _) -> Format.pp_print_list ~pp_sep pp_print_month ppf v
  | Vect (Date, _) -> Format.pp_print_list ~pp_sep pp_print_date ppf v
  | Vect (Timespan, _) -> Format.pp_print_list ~pp_sep pp_print_timespan ppf v
  | Vect (Minute, _) -> Format.pp_print_list ~pp_sep pp_print_minute ppf v
  | Vect (Second, _) -> Format.pp_print_list ~pp_sep pp_print_second ppf v
  | Vect (Time, _) -> Format.pp_print_list ~pp_sep pp_print_time ppf v
  | Vect (Lambda, _) -> Format.pp_print_list ~pp_sep pp_print_lambda ppf v

  | String (Byte, _) -> Format.pp_print_string ppf v
  | String (Char, _) -> Format.pp_print_string ppf v
  | String _ -> invalid_arg "pp: unsupported string type"

  | Err -> Format.pp_print_string ppf v
  | Union _ -> Format.pp_print_string ppf "<union>"

(*---------------------------------------------------------------------------
   Copyright (c) 2018 Vincent Bernardoff

   Permission to use, copy, modify, and/or distribute this software for any
   purpose with or without fee is hereby granted, provided that the above
   copyright notice and this permission notice appear in all copies.

   THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
   WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
   MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
   ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
   WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
   ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
   OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
  ---------------------------------------------------------------------------*)
