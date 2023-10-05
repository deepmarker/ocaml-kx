(*---------------------------------------------------------------------------
  Copyright (c) 2018 Vincent Bernardoff. All rights reserved.
  Distributed under the ISC license, see terms at the end of the file.
  ---------------------------------------------------------------------------*)

open Sexplib.Std

let ng = Uuidm.nil
let nh = ~-0x8000
let wh = 0x7fff
let minus_wh = ~-0x7fff
let ni = Int32.min_int
let wi = Int32.max_int
let minus_wi = Int32.(succ min_int)
let nj = Int64.min_int
let wj = Int64.max_int
let minus_wj = Int64.(succ min_int)
let nf = nan
let wf = infinity
let minus_wf = neg_infinity

let ptime_neginf =
  let open Ptime in
  let d, ps = Span.to_d_ps (to_span min) in
  match of_span (Span.unsafe_of_d_ps (d, Int64.succ ps)) with
  | None -> assert false
  | Some t -> t
;;

let millenium =
  match Ptime.of_date (2000, 1, 1) with
  | None -> assert false
  | Some t -> t
;;

let of_day_exn d =
  match Ptime.Span.of_d_ps (Int32.to_int d, 0L) with
  | None -> invalid_arg "Ptime.Span.of_d_ps"
  | Some t -> t
;;

let add_span_exn t s =
  match Ptime.add_span t s with
  | None -> invalid_arg "Ptime.add_span"
  | Some t -> t
;;

let date_of_int32 d =
  if d = ni
  then 0, 1, 1
  else if d = wi
  then 9999, 12, 12
  else if d = Int32.neg wi
  then 0, 1, 2
  else Ptime.to_date (add_span_exn millenium (of_day_exn d))
;;

let int32_of_date = function
  | 0, 1, 1 -> ni
  | 0, 1, 2 -> Int32.neg wi
  | d ->
    (match Ptime.of_date d with
     | None -> invalid_arg "int32_of_date"
     | Some t -> Int32.of_int (fst Ptime.(Span.to_d_ps (diff t millenium))))
;;

let kx_epoch, kx_epoch_span =
  match Ptime.of_date (2000, 1, 1) with
  | None -> assert false
  | Some t -> t, Ptime.to_span t
;;

let day_in_ns d = Int64.(mul (of_int (d * 24 * 3600)) 1_000_000_000L)
let day_in_ms d = Int32.(mul (of_int (d * 24 * 3600)) 1_000l)
let day_in_minutes d = Int32.of_int (d * 24 * 60)
let day_in_seconds d = Int32.of_int (d * 24 * 3600)
let ps_count_in_day = 86_400_000_000_000_000L

let timestamp_of_int64 = function
  | i when Int64.(equal i nj) -> Ptime.min
  | i when Int64.(equal i wj) -> Ptime.max
  | i when Int64.(equal i (neg wj)) -> ptime_neginf
  | nanos_since_kxepoch ->
    let one_day_in_ns = day_in_ns 1 in
    let days_since_kxepoch = Int64.(to_int (div nanos_since_kxepoch one_day_in_ns)) in
    let remaining_ps = Int64.(mul (rem nanos_since_kxepoch one_day_in_ns) 1_000L) in
    let d, ps =
      if remaining_ps < 0L
      then pred days_since_kxepoch, Int64.add ps_count_in_day remaining_ps
      else days_since_kxepoch, remaining_ps
    in
    let span = Ptime.Span.v (d, ps) in
    (match Ptime.add_span kx_epoch span with
     | None -> invalid_arg "timestamp_of_int64"
     | Some ts -> ts)
;;

let min_kx_ts = timestamp_of_int64 Int64.(add min_int 2L)
let max_kx_ts = timestamp_of_int64 Int64.(pred max_int)

let int64_of_timestamp = function
  | ts when Ptime.(equal ts min) -> nj
  | ts when Ptime.(equal ts max) -> wj
  | ts when Ptime.(equal ts ptime_neginf) -> Int64.neg wj
  | ts when Ptime.is_later ts ~than:max_kx_ts ->
    invalid_arg "ts is too big to be representable by kdb+"
  | ts when Ptime.is_earlier ts ~than:min_kx_ts ->
    invalid_arg "ts is too small to be representable by kdb+"
  | ts ->
    let span_since_kxepoch = Ptime.(Span.sub (to_span ts) kx_epoch_span) in
    let d, ps = Ptime.Span.to_d_ps span_since_kxepoch in
    Int64.(add (day_in_ns d) (div ps 1_000L))
;;

let min_span = Ptime.to_span Ptime.min
let succ_min_span = Ptime.to_span (Option.get Ptime.(add_span min (Span.v (0, 1L))))

let span_of_ns = function
  | -9223372036854775808L -> min_span
  | -9223372036854775807L -> succ_min_span
  | ns ->
    let open Int64 in
    let is_negative = ns < 0L in
    let ns = abs ns in
    let d = div ns (day_in_ns 1) in
    let rem_ns = rem ns (day_in_ns 1) in
    let span = Ptime.Span.v (to_int d, mul rem_ns 1_000L) in
    if is_negative then Ptime.Span.neg span else span
;;

let jd_posix_epoch = 2_440_588 (* the Julian day of the POSIX epoch *)
let jd_ptime_min = 1_721_060 (* the Julian day of Ptime.min *)

(* let jd_ptime_max = 5_373_484                  (\* the Julian day of Ptime.max *\) *)
let day_min = jd_ptime_min - jd_posix_epoch

(* let day_max = jd_ptime_max - jd_posix_epoch *)

let ns_of_span span =
  let open Int64 in
  let d, ps = Ptime.Span.to_d_ps span in
  match d, ps with
  | _, 0L when d = day_min -> min_int
  | _, 1L when d = day_min -> succ min_int
  | _ -> add (mul (of_int d) (day_in_ns 1)) (div ps 1_000L)
;;

let span_of_ms = function
  | -2147483648l -> min_span
  | -2147483647l -> succ_min_span
  | ms ->
    let open Int32 in
    let is_negative = ms < 0l in
    let d = div ms (day_in_ms 1) in
    let ps = rem ms (day_in_ms 1) in
    let span = Ptime.Span.v (to_int d, Int64.(div (of_int32 ps) 1_000_000_000L)) in
    if is_negative then Ptime.Span.neg span else span
;;

let ms_of_span span =
  let open Int32 in
  let d, ps = Ptime.Span.to_d_ps span in
  match d, ps with
  | _, 0L when d = day_min -> min_int
  | _, 1L when d = day_min -> succ min_int
  | _ -> add (mul (of_int d) (day_in_ms 1)) Int64.(to_int32 (div ps 1_000_000_000L))
;;

let int32_of_month (y, m, _) = Int32.of_int (((y - 2000) * 12) + pred m)

let month_of_int32 m =
  let open Int32 in
  let y = div m 12l in
  let rem_m = rem m 12l in
  to_int (add 2000l y), to_int (succ rem_m), 0
;;

let minute_of_span span =
  let d, ps = Ptime.Span.to_d_ps span in
  Int32.(add (of_int (d * 24 * 60)) Int64.(div ps 60_000_000_000_000L |> to_int32))
;;

let second_of_span span =
  let d, ps = Ptime.Span.to_d_ps span in
  Int32.(add (of_int (d * 24 * 60)) Int64.(div ps 1_000_000_000_000L |> to_int32))
;;

let span_of_minute i =
  let d = Int32.(to_int (div i (day_in_minutes 1))) in
  let m =
    let open Int64 in
    mul (mul 60L 1_000_000_000_000L) (of_int32 (Int32.rem i (day_in_minutes 1)))
  in
  Ptime.Span.unsafe_of_d_ps (d, m)
;;

let span_of_second i =
  let d = Int32.(to_int (div i (day_in_seconds 1))) in
  let m =
    let open Int64 in
    mul (mul 60L 1_000_000_000_000L) (of_int32 (Int32.rem i (day_in_seconds 1)))
  in
  Ptime.Span.unsafe_of_d_ps (d, m)
;;

let nn = span_of_ns nj
let wn = span_of_ns wj
let minus_wn = span_of_ns (Int64.neg wj)
let np = Option.get (Ptime.of_span nn)
let wp = Option.get (Ptime.of_span wn)
let minus_wp = Option.get (Ptime.of_span minus_wn)
let nt = span_of_ms ni
let wt = span_of_ms wi
let minus_wt = span_of_ms (Int32.neg wi)
let nm = month_of_int32 ni
let wm = month_of_int32 wi
let minus_wm = month_of_int32 (Int32.neg wi)
let nd = date_of_int32 ni
let wd = date_of_int32 wi
let minus_wd = date_of_int32 (Int32.neg wi)
let nu = span_of_minute ni
let wu = span_of_minute wi
let minus_wu = span_of_minute (Int32.neg wi)
let nv = span_of_second ni
let wv = span_of_second wi
let minus_wv = span_of_second (Int32.neg wi)

module Maybe = struct
  let maybe eqf eq v = if eqf eq v then None else Some v
  let of_g = maybe Uuidm.equal ng
  let of_h = maybe ( = ) nh
  let of_i = maybe ( = ) ni
  let of_j = maybe ( = ) nj
  let of_f = maybe Float.equal nf
  let of_p = maybe Ptime.equal np
  let of_s = maybe String.equal ""
  let of_n = maybe Ptime.Span.equal nn
  let to_g = Option.value ~default:ng
  let to_h = Option.value ~default:nh
  let to_i = Option.value ~default:ni
  let to_j = Option.value ~default:nj
  let to_f = Option.value ~default:nf
  let to_p = Option.value ~default:np
  let to_n = Option.value ~default:nn
  let to_s = Option.value ~default:""
end

type _ typ =
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
  | Timespan : Ptime.Span.t typ
  | Minute : Ptime.Span.t typ
  | Second : Ptime.Span.t typ
  | Time : Ptime.Span.t typ
  | Lambda : (string * string) typ
  | UnaryPrim : int typ
  | Operator : int typ
  | Over : int typ

type (_, _) eq = Eq : ('a, 'a) eq

let eq_typ : type a b. a typ -> b typ -> (a, b) eq option =
  fun a b ->
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
  | UnaryPrim, UnaryPrim -> Some Eq
  | Operator, Operator -> Some Eq
  | Over, Over -> Some Eq
  | _ -> None
;;

let eq_typ_val : type a b. a typ -> a -> b typ -> b -> (a, b) eq option =
  fun a x b y ->
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
  | Lambda, Lambda when String.equal (fst x) (fst y) && String.equal (snd x) (snd y) ->
    Some Eq
  | UnaryPrim, UnaryPrim when Int.equal x y -> Some Eq
  | Operator, Operator when Int.equal x y -> Some Eq
  | Over, Over when Int.equal x y -> Some Eq
  | _ -> None
;;

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
;;

let attribute =
  Angstrom.satisfy (function
    | '\x00' .. '\x04' -> true
    | _ -> false)
;;

type _ w =
  | Unit : unit w
  | Err : string w
  | Atom : 'a typ -> 'a w
  | Vect : 'a typ * attribute option -> 'a array w
  | String : char typ * attribute option -> string w
  | List : 'a w * attribute option -> 'a array w
  | Tup : 'a w * attribute option -> 'a w
  | Tups : 'a w * 'b w * attribute option -> ('a * 'b) w
  | Dict : 'a w * 'b w * bool -> ('a * 'b) w
  | Table : 'b w * bool -> (string array * 'b) w
  | Conv : ('a -> 'b) * ('b -> 'a) * 'b w -> 'a w
  | Union : 'a case list -> 'a w

and _ case =
  | Case :
      { encoding : 'a w
      ; proj : 't -> 'a option
      ; inj : 'a -> 't
      }
      -> 't case

let case encoding proj inj = Case { encoding; proj; inj }
let union cases = Union cases

let rec is_list_type : type a. a w -> bool = function
  | Tup _ -> true
  | Tups _ -> true
  | List _ -> true
  | Vect _ -> true
  | Table _ -> true
  | Conv (_, _, w) -> is_list_type w
  | _ -> false
;;

let rec equal_w : type a b. a w -> b w -> bool =
  fun a b ->
  match a, b with
  | Atom a, Atom b -> eq_typ a b <> None
  | Vect (a, aa), Vect (b, ba) -> eq_typ a b <> None && aa = ba
  | String (a, aa), String (b, ba) -> eq_typ a b <> None && aa = ba
  | List (a, aa), List (b, ba) -> equal_w a b && aa = ba
  | Tup (a, aa), Tup (b, ba) -> equal_w a b && aa = ba
  | Tups (a, b, aa), Tups (c, d, ba) -> equal_w a c && equal_w b d && aa = ba
  | Dict (a, b, s1), Dict (c, d, s2) -> equal_w a c && equal_w b d && s1 = s2
  | Table (a, s1), Table (b, s2) -> equal_w a b && s1 = s2
  | Conv (_, _, a), Conv (_, _, b) -> equal_w a b
  | Union a, Union b -> List.fold_left2 (fun a c1 c2 -> a && equal_case c1 c2) true a b
  | _ -> false

and equal_case : type a b. a case -> b case -> bool =
  fun (Case { encoding; _ }) (Case { encoding = e2; _ }) -> equal_w encoding e2
;;

let rec equal : type a b. a w -> a -> b w -> b -> bool =
  fun aw x bw y ->
  match aw, bw with
  | Atom a, Atom b -> eq_typ_val a x b y <> None
  | Vect (a, aa), Vect (b, ba) ->
    aa = ba
    && Array.length x = Array.length y
    &&
      (try
         ArrayLabels.iter2
           ~f:(fun x y -> if eq_typ_val a x b y = None then raise_notrace Exit)
           x
           y;
         true
       with
      | Exit -> false)
  | String _, String _ -> String.equal x y
  | List (a, aa), List (b, ba) ->
    aa = ba
    && Array.length x = Array.length y
    &&
      (try
         ArrayLabels.iter2
           ~f:(fun x y -> if not (equal a x b y) then raise_notrace Exit)
           x
           y;
         true
       with
      | Exit -> false)
  | Tup (a, aa), Tup (b, ba) -> aa = ba && equal a x b y
  | Tups (a, b, aa), Tups (c, d, ba) ->
    let x1, x2 = x in
    let y1, y2 = y in
    aa = ba && equal a x1 c y1 && equal b x2 d y2
  | Dict (a, b, s1), Dict (c, d, s2) ->
    let x1, x2 = x in
    let y1, y2 = y in
    equal a x1 c y1 && equal b x2 d y2 && s1 = s2
  | Table (a, s1), Table (b, s2) ->
    let x1, x2 = x in
    let y1, y2 = y in
    let v = Vect (Symbol, None) in
    equal v x1 v y1 && equal a x2 b y2 && s1 = s2
  | Conv (p1, _, a), Conv (p2, _, b) -> equal a (p1 x) b (p2 y)
  | Union c1, Union c2 ->
    equal_w aw bw
    && List.fold_left2
         (fun a (Case { encoding; proj; _ }) (Case { encoding = e2; proj = proj2; _ }) ->
           a
           &&
           match
             ( (try proj x with
                | _ -> None)
             , try proj2 y with
               | _ -> None )
           with
           | None, None -> true
           | Some a, Some b -> equal encoding a e2 b
           | _ -> false)
         true
         c1
         c2
  | _ -> false
;;

let unit = Unit
let bool = Boolean
let guid = Guid
let byte = Byte
let short = Short
let int = Int
let long = Long
let real = Real
let float = Float
let char = Char
let sym = Symbol
let timestamp = Timestamp
let month = Month
let date = Date
let timespan = Timespan
let minute = Minute
let second = Second
let time = Time
let lambda = Lambda
let unaryprim = UnaryPrim
let operator = Operator
let over = Over

module Unary = struct
  let id = 0x00
  let neg = 0x02
  let sum = 0x19
  let prd = 0x1a
end

module Op = struct
  let plus = 1
  let minus = 2
  let times = 3
  let divide = 4
  let min = 5
  let max = 6
  let fill = 7
  let eq = 8
  let le = 9
  let ge = 10
  let cast = 11
  let join = 12
  let take = 13
  let cut = 14
  let mtch = 15
  let key = 16
  let find = 17
  let at = 18
  let dot = 19
  let text = 20
  let binary = 21
end

let conv proj inj a = Conv (proj, inj, a)
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
    (fun (a, b, c) -> a, (b, c))
    (fun (a, (b, c)) -> a, b, c)
    (tups (tup a attr) (t2 ?attr b c) attr)
;;

let t4 ?attr a b c d =
  conv
    (fun (a, b, c, d) -> a, (b, c, d))
    (fun (a, (b, c, d)) -> a, b, c, d)
    (tups (tup a attr) (t3 ?attr b c d) attr)
;;

let t5 ?attr a b c d e =
  conv
    (fun (a, b, c, d, e) -> a, (b, c, d, e))
    (fun (a, (b, c, d, e)) -> a, b, c, d, e)
    (tups (tup a attr) (t4 ?attr b c d e) attr)
;;

let t6 ?attr a b c d e f =
  conv
    (fun (a, b, c, d, e, f) -> a, (b, c, d, e, f))
    (fun (a, (b, c, d, e, f)) -> a, b, c, d, e, f)
    (tups (tup a attr) (t5 ?attr b c d e f) attr)
;;

let t7 ?attr a b c d e f g =
  conv
    (fun (a, b, c, d, e, f, g) -> a, (b, c, d, e, f, g))
    (fun (a, (b, c, d, e, f, g)) -> a, b, c, d, e, f, g)
    (tups (tup a attr) (t6 ?attr b c d e f g) attr)
;;

let t8 ?attr a b c d e f g h =
  conv
    (fun (a, b, c, d, e, f, g, h) -> a, (b, c, d, e, f, g, h))
    (fun (a, (b, c, d, e, f, g, h)) -> a, b, c, d, e, f, g, h)
    (tups (tup a attr) (t7 ?attr b c d e f g h) attr)
;;

let t9 ?attr a b c d e f g h i =
  conv
    (fun (a, b, c, d, e, f, g, h, i) -> a, (b, c, d, e, f, g, h, i))
    (fun (a, (b, c, d, e, f, g, h, i)) -> a, b, c, d, e, f, g, h, i)
    (tups (tup a attr) (t8 ?attr b c d e f g h i) attr)
;;

let t10 ?attr a b c d e f g h i j =
  conv
    (fun (a, b, c, d, e, f, g, h, i, j) -> a, (b, c, d, e, f, g, h, i, j))
    (fun (a, (b, c, d, e, f, g, h, i, j)) -> a, b, c, d, e, f, g, h, i, j)
    (tups (tup a attr) (t9 ?attr b c d e f g h i j) attr)
;;

let merge_tups t1 t2 =
  let rec is_tup : type t. t w -> attribute option option = function
    | Tup (_, a) -> Some a
    | Tups (_, _, a) (* by construction *) -> Some a
    | Conv (_, _, t) -> is_tup t
    | _ -> None
  in
  match is_tup t1, is_tup t2 with
  | Some a, Some b when a = b -> Tups (t1, t2, a)
  | _ -> invalid_arg "merge_tups"
;;

let dict ?(sorted = false) k v =
  if not (is_list_type k && is_list_type v)
  then invalid_arg "dict keys and values must be lists";
  Dict (k, v, sorted)
;;

let cd1 ?(sorted = false) ?attr v1 = Dict (v ?attr sym, t1 (a v1), sorted)
let cd2 ?(sorted = false) ?attr v1 v2 = Dict (v ?attr sym, t2 (a v1) (a v2), sorted)

let cd3 ?(sorted = false) ?attr v1 v2 v3 =
  Dict (v ?attr sym, t3 (a v1) (a v2) (a v3), sorted)
;;

let cd4 ?(sorted = false) ?attr v1 v2 v3 v4 =
  Dict (v ?attr sym, t4 (a v1) (a v2) (a v3) (a v4), sorted)
;;

let cd5 ?(sorted = false) ?attr v1 v2 v3 v4 v5 =
  Dict (v ?attr sym, t5 (a v1) (a v2) (a v3) (a v4) (a v5), sorted)
;;

let cd6 ?(sorted = false) ?attr v1 v2 v3 v4 v5 v6 =
  Dict (v ?attr sym, t6 (a v1) (a v2) (a v3) (a v4) (a v5) (a v6), sorted)
;;

let cd7 ?(sorted = false) ?attr v1 v2 v3 v4 v5 v6 v7 =
  Dict (v ?attr sym, t7 (a v1) (a v2) (a v3) (a v4) (a v5) (a v6) (a v7), sorted)
;;

let cd8 ?(sorted = false) ?attr v1 v2 v3 v4 v5 v6 v7 v8 =
  Dict (v ?attr sym, t8 (a v1) (a v2) (a v3) (a v4) (a v5) (a v6) (a v7) (a v8), sorted)
;;

let cd9 ?(sorted = false) ?attr v1 v2 v3 v4 v5 v6 v7 v8 v9 =
  Dict
    ( v ?attr sym
    , t9 (a v1) (a v2) (a v3) (a v4) (a v5) (a v6) (a v7) (a v8) (a v9)
    , sorted )
;;

let table ?(sorted = false) vs =
  if not (is_list_type vs) then invalid_arg "table keys and values must be lists";
  Table (vs, sorted)
;;

let table1 ?(sorted = false) ?attr v1 = Table (t1 (v ?attr v1), sorted)

let table2 ?(sorted = false) v1 v2 =
  let attr = if sorted then Some Parted else None in
  Table (t2 (v ?attr v1) (v v2), sorted)
;;

let table3 ?(sorted = false) v1 v2 v3 =
  let attr = if sorted then Some Parted else None in
  Table (t3 (v ?attr v1) (v v2) (v v3), sorted)
;;

let table4 ?(sorted = false) v1 v2 v3 v4 =
  let attr = if sorted then Some Parted else None in
  Table (t4 (v ?attr v1) (v v2) (v v3) (v v4), sorted)
;;

let table5 ?(sorted = false) v1 v2 v3 v4 v5 =
  let attr = if sorted then Some Parted else None in
  Table (t5 (v ?attr v1) (v v2) (v v3) (v v4) (v v5), sorted)
;;

let table6 ?(sorted = false) v1 v2 v3 v4 v5 v6 =
  let attr = if sorted then Some Parted else None in
  Table (t6 (v ?attr v1) (v v2) (v v3) (v v4) (v v5) (v v6), sorted)
;;

let table7 ?(sorted = false) v1 v2 v3 v4 v5 v6 v7 =
  let attr = if sorted then Some Parted else None in
  Table (t7 (v ?attr v1) (v v2) (v v3) (v v4) (v v5) (v v6) (v v7), sorted)
;;

let table8 ?(sorted = false) v1 v2 v3 v4 v5 v6 v7 v8 =
  let attr = if sorted then Some Parted else None in
  Table (t8 (v ?attr v1) (v v2) (v v3) (v v4) (v v5) (v v6) (v v7) (v v8), sorted)
;;

let table9 ?(sorted = false) v1 v2 v3 v4 v5 v6 v7 v8 v9 =
  let attr = if sorted then Some Parted else None in
  Table (t9 (v ?attr v1) (v v2) (v v3) (v v4) (v v5) (v v6) (v v7) (v v8) (v v9), sorted)
;;

(* let string_of_chars a = String.init (Array.length a) (Array.get a) *)
let pp_print_month ppf (y, m, _) = Format.fprintf ppf "%d.%dm" y m
let pp_print_date ppf (y, m, d) = Format.fprintf ppf "%d.%d.%d" y m d

(* let pp_print_symbols ppf syms = Array.iter (fun sym -> Format.fprintf ppf "`%s" sym) syms *)

let pp_print_timestamp ppf = function
  | ts when ts = Ptime.min -> Format.pp_print_string ppf "0Np"
  | ts when ts = Ptime.max -> Format.pp_print_string ppf "0Wp"
  | ts when ts = ptime_neginf -> Format.pp_print_string ppf "-0Wp"
  | ts -> Format.fprintf ppf "%a" (Ptime.pp_rfc3339 ~frac_s:9 ()) ts
;;

let pp_print_lambda ppf (ctx, lambda) = Format.pp_print_string ppf (ctx ^ lambda)
let pp_print_unaryprim ppf i = Format.fprintf ppf "<unary %d>" i
let pp_print_operator ppf i = Format.fprintf ppf "<op %d>" i
let pp_print_over ppf i = Format.fprintf ppf "<%d/>" i

module type FE = module type of Faraday.BE

let rec construct_list : type a. (module FE) -> Faraday.t -> a w -> a -> int =
  fun e buf w a ->
  match w with
  | Tup (w, _) ->
    construct e buf w a;
    1
  | Tups (hw, tw, _) ->
    let lenh = construct_list e buf hw (fst a) in
    let lent = construct_list e buf tw (snd a) in
    lenh + lent
  | Conv (proj, _, w) -> construct_list e buf w (proj a)
  | _ -> assert false

and construct : type a. (module FE) -> Faraday.t -> a w -> a -> unit =
  fun e buf w a ->
  let open Faraday in
  let module FE = (val e : FE) in
  match w with
  | Union cases ->
    let rec do_cases = function
      | [] -> invalid_arg "construct: union"
      | Case { encoding; proj; _ } :: rest ->
        (match proj a with
         | Some t -> construct e buf encoding t
         | None -> do_cases rest
         | exception _ -> do_cases rest)
    in
    do_cases cases
  | Err ->
    write_char buf '\x80';
    write_string buf a;
    write_char buf '\x00'
  | List (w', attr) ->
    write_char buf '\x00';
    write_char buf (char_of_attribute attr);
    FE.write_uint32 buf (Int32.of_int (Array.length a));
    Array.iter (construct e buf w') a
  | Tup (ww, attr) ->
    write_char buf '\x00';
    write_char buf (char_of_attribute attr);
    FE.write_uint32 buf 1l;
    construct e buf ww a
  | Tups (_, _, attr) ->
    let buf' = create 13 in
    let len = construct_list e buf' w a in
    write_char buf '\x00';
    write_char buf (char_of_attribute attr);
    FE.write_uint32 buf (Int32.of_int len);
    schedule_bigstring buf (serialize_to_bigstring buf')
  | Dict (k, v, sorted) ->
    let x, y = a in
    write_char buf (if sorted then '\x7f' else '\x63');
    construct e buf k x;
    construct e buf v y
  | Table (vs, sorted) ->
    write_char buf '\x62';
    write_char buf (if sorted then '\x01' else '\x00');
    construct e buf (Dict (v sym, vs, false)) a
  | Conv (proj, _, ww) -> construct e buf ww (proj a)
  | Unit ->
    write_char buf '\x65';
    write_char buf '\x00'
  | Atom Boolean ->
    write_char buf '\xff';
    (match a with
     | false -> write_char buf '\x00'
     | true -> write_char buf '\x01')
  | Atom Guid ->
    write_char buf '\xfe';
    write_string buf (Uuidm.to_bytes a)
  | Atom Byte ->
    write_char buf '\xfc';
    write_char buf a
  | Atom Short ->
    write_char buf '\xfb';
    FE.write_uint16 buf a
  | Atom Int ->
    write_char buf '\xfa';
    FE.write_uint32 buf a
  | Atom Long ->
    write_char buf '\xf9';
    FE.write_uint64 buf a
  | Atom Real ->
    write_char buf '\xf8';
    FE.write_float buf a
  | Atom Float ->
    write_char buf '\xf7';
    FE.write_double buf a
  | Atom Char ->
    write_char buf '\xf6';
    write_char buf a
  | Atom Symbol ->
    write_char buf '\xf5';
    write_string buf a;
    write_char buf '\x00'
  | Atom Timestamp ->
    write_char buf '\xf4';
    FE.write_uint64 buf (int64_of_timestamp a)
  | Atom Month ->
    write_char buf '\xf3';
    FE.write_uint32 buf (int32_of_month a)
  | Atom Date ->
    write_char buf '\xf2';
    FE.write_uint32 buf (int32_of_date a)
  | Atom Timespan ->
    write_char buf '\xf0';
    FE.write_uint64 buf (ns_of_span a)
  | Atom Minute ->
    write_char buf '\xef';
    FE.write_uint32 buf (minute_of_span a)
  | Atom Second ->
    write_char buf '\xee';
    FE.write_uint32 buf (second_of_span a)
  | Atom Time ->
    write_char buf '\xed';
    FE.write_uint32 buf (ms_of_span a)
  | Atom Lambda ->
    write_char buf '\x64';
    write_string buf (fst a);
    write_char buf '\x00';
    construct e buf (String (Char, None)) (snd a)
  | Atom UnaryPrim ->
    write_char buf '\x65';
    write_char buf (Char.chr a)
  | Atom Operator ->
    write_char buf '\x66';
    write_char buf (Char.chr a)
  | Atom Over ->
    write_char buf '\x6b';
    write_char buf '\x66';
    write_char buf (Char.chr a)
  | Vect (Lambda, attr) ->
    (match attr with
     | None | Some Grouped -> ()
     | _ -> invalid_arg "lambda cannot have attr except grouped");
    write_char buf '\x00';
    write_char buf (char_of_attribute attr);
    FE.write_uint32 buf (Int32.of_int (Array.length a));
    Array.iter (fun lam -> construct e buf (Atom Lambda) lam) a
  | Vect (UnaryPrim, attr) ->
    (match attr with
     | None | Some Grouped -> ()
     | _ -> invalid_arg "unaryprim cannot have attr except grouped");
    write_char buf '\x00';
    write_char buf (char_of_attribute attr);
    FE.write_uint32 buf (Int32.of_int (Array.length a));
    Array.iter (fun up -> construct e buf (Atom UnaryPrim) up) a
  | Vect (Operator, attr) ->
    (match attr with
     | None | Some Grouped -> ()
     | _ -> invalid_arg "operator cannot have attr except grouped");
    write_char buf '\x00';
    write_char buf (char_of_attribute attr);
    FE.write_uint32 buf (Int32.of_int (Array.length a));
    Array.iter (fun op -> construct e buf (Atom Operator) op) a
  | Vect (Over, attr) ->
    (match attr with
     | None | Some Grouped -> ()
     | _ -> invalid_arg "over cannot have attr except grouped");
    write_char buf '\x00';
    write_char buf (char_of_attribute attr);
    FE.write_uint32 buf (Int32.of_int (Array.length a));
    Array.iter (fun op -> construct e buf (Atom Over) op) a
  | Vect (Boolean, attr) ->
    write_char buf '\x01';
    write_char buf (char_of_attribute attr);
    FE.write_uint32 buf (Int32.of_int (Array.length a));
    Array.iter
      (function
       | false -> write_char buf '\x00'
       | true -> write_char buf '\x01')
      a
  | String (Byte, attr) ->
    write_char buf '\x04';
    write_char buf (char_of_attribute attr);
    FE.write_uint32 buf (Int32.of_int (String.length a));
    write_string buf a
  | Vect (Byte, attr) ->
    write_char buf '\x04';
    write_char buf (char_of_attribute attr);
    FE.write_uint32 buf (Int32.of_int (Array.length a));
    Array.iter (write_char buf) a
  | String (Char, attr) ->
    write_char buf '\x0a';
    write_char buf (char_of_attribute attr);
    FE.write_uint32 buf (Int32.of_int (String.length a));
    write_string buf a
  | Vect (Char, attr) ->
    write_char buf '\x0a';
    write_char buf (char_of_attribute attr);
    FE.write_uint32 buf (Int32.of_int (Array.length a));
    Array.iter (write_char buf) a
  | Vect (Short, attr) ->
    write_char buf '\x05';
    write_char buf (char_of_attribute attr);
    FE.write_uint32 buf (Int32.of_int (Array.length a));
    Array.iter (FE.write_uint16 buf) a
  | Vect (Int, attr) ->
    write_char buf '\x06';
    write_char buf (char_of_attribute attr);
    FE.write_uint32 buf (Int32.of_int (Array.length a));
    Array.iter (FE.write_uint32 buf) a
  | Vect (Long, attr) ->
    write_char buf '\x07';
    write_char buf (char_of_attribute attr);
    FE.write_uint32 buf (Int32.of_int (Array.length a));
    Array.iter (FE.write_uint64 buf) a
  | Vect (Real, attr) ->
    write_char buf '\x08';
    write_char buf (char_of_attribute attr);
    FE.write_uint32 buf (Int32.of_int (Array.length a));
    Array.iter (FE.write_float buf) a
  | Vect (Float, attr) ->
    write_char buf '\x09';
    write_char buf (char_of_attribute attr);
    FE.write_uint32 buf (Int32.of_int (Array.length a));
    Array.iter (FE.write_double buf) a
  | Vect (Symbol, attr) ->
    write_char buf '\x0b';
    write_char buf (char_of_attribute attr);
    FE.write_uint32 buf (Int32.of_int (Array.length a));
    Array.iter
      (fun s ->
        write_string buf s;
        write_char buf '\x00')
      a
  | Vect (Guid, attr) ->
    write_char buf '\x02';
    write_char buf (char_of_attribute attr);
    FE.write_uint32 buf (Int32.of_int (Array.length a));
    Array.iter (fun s -> write_string buf (Uuidm.to_bytes s)) a
  | Vect (Timestamp, attr) ->
    write_char buf '\x0c';
    write_char buf (char_of_attribute attr);
    FE.write_uint32 buf (Int32.of_int (Array.length a));
    Array.iter (fun ts -> FE.write_uint64 buf (int64_of_timestamp ts)) a
  | Vect (Month, attr) ->
    write_char buf '\x0d';
    write_char buf (char_of_attribute attr);
    FE.write_uint32 buf (Int32.of_int (Array.length a));
    Array.iter (fun m -> FE.write_uint32 buf (int32_of_month m)) a
  | Vect (Date, attr) ->
    write_char buf '\x0e';
    write_char buf (char_of_attribute attr);
    FE.write_uint32 buf (Int32.of_int (Array.length a));
    Array.iter (fun m -> FE.write_uint32 buf (int32_of_date m)) a
  | Vect (Timespan, attr) ->
    write_char buf '\x10';
    write_char buf (char_of_attribute attr);
    FE.write_uint32 buf (Int32.of_int (Array.length a));
    Array.iter (fun a -> FE.write_uint64 buf (ns_of_span a)) a
  | Vect (Minute, attr) ->
    write_char buf '\x11';
    write_char buf (char_of_attribute attr);
    FE.write_uint32 buf (Int32.of_int (Array.length a));
    Array.iter (fun m -> FE.write_uint32 buf (minute_of_span m)) a
  | Vect (Second, attr) ->
    write_char buf '\x12';
    write_char buf (char_of_attribute attr);
    FE.write_uint32 buf (Int32.of_int (Array.length a));
    Array.iter (fun m -> FE.write_uint32 buf (second_of_span m)) a
  | Vect (Time, attr) ->
    write_char buf '\x13';
    write_char buf (char_of_attribute attr);
    FE.write_uint32 buf (Int32.of_int (Array.length a));
    Array.iter (fun a -> FE.write_uint32 buf (ms_of_span a)) a
  | String _ -> assert false
;;

type msgtyp =
  | Async
  | Sync
  | Response
[@@deriving sexp_of]

let char_of_msgtyp = function
  | Async -> '\x00'
  | Sync -> '\x01'
  | Response -> '\x02'
;;

let msgtyp_of_int = function
  | 0 -> Async
  | 1 -> Sync
  | 2 -> Response
  | _ -> invalid_arg "msgtyp_of_int"
;;

type hdr =
  { big_endian : bool
  ; typ : msgtyp
  ; compressed : bool
  ; len : int32
  }
[@@deriving sexp_of]

let pp_print_hdr ppf t = Format.fprintf ppf "%a" Sexplib.Sexp.pp (sexp_of_hdr t)

let set_int32 ~big_endian =
  Bigstringaf.(if big_endian then set_int32_be else set_int32_le)
;;

let ppincr x =
  incr x;
  !x
;;

let incrpp x =
  let v = !x in
  incr x;
  v
;;

let set_int8 t p i = Bigstringaf.set t p (Char.chr (0xff land i))
let get_int8 t p = Char.code (Bigstringaf.get t p)

let compress ?(big_endian = Sys.big_endian) uncompressed =
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
  Bigstringaf.blit uncompressed ~src_off:0 compressed ~dst_off:0 ~len:4;
  set_int8 compressed 2 1;
  set_int32 ~big_endian compressed 8 (Int32.of_int uncompLen);
  while !s < uncompLen do
    if 0 = !i
    then (
      if !d > compLen - 17 then raise_notrace Exit;
      i := 1;
      set_int8 compressed !c !f;
      c := incrpp d;
      f := 0);
    let clause2 () =
      h := get_int8 uncompressed !s lxor get_int8 uncompressed (succ !s);
      p := a.(!h);
      !p
    in
    g
      := !s > uncompLen - 3
         || 0 = clause2 ()
         || 0 <> get_int8 uncompressed !s lxor get_int8 uncompressed !p;
    if 0 < !s0
    then (
      a.(!h0) <- !s0;
      s0 := 0);
    if !g
    then (
      h0 := !h;
      s0 := !s;
      set_int8 compressed (incrpp d) (get_int8 uncompressed (incrpp s)))
    else (
      a.(!h) <- !s;
      f := !f lor !i;
      p := !p + 2;
      s := !s + 2;
      r := !s;
      let q = min (!s + 255) uncompLen in
      while get_int8 uncompressed !p = get_int8 uncompressed !s && ppincr s < q do
        incr p
      done;
      set_int8 compressed (incrpp d) !h;
      set_int8 compressed (incrpp d) (!s - !r));
    i := !i * 2 mod 256
  done;
  set_int8 compressed !c !f;
  set_int32 ~big_endian compressed 4 (Int32.of_int !d);
  Bigstringaf.sub compressed ~off:0 ~len:!d
;;

let construct_bigstring
  ?(comp = false)
  ?(big_endian = Sys.big_endian)
  ~typ
  ?(buf = Faraday.create 4096)
  w
  x
  =
  let module FE =
    (val Faraday.(if big_endian then (module BE : FE) else (module LE : FE)))
  in
  construct (module FE) buf w x;
  let len = Faraday.pending_bytes buf in
  let uncompressed = Bigstringaf.create (8 + len) in
  Bigstringaf.set uncompressed 0 (if big_endian then '\x00' else '\x01');
  Bigstringaf.set uncompressed 1 (char_of_msgtyp typ);
  Bigstringaf.set_int16_be uncompressed 2 0;
  set_int32 ~big_endian uncompressed 4 (Int32.of_int (8 + len));
  let _ =
    Faraday.serialize buf (fun iovecs ->
      let dst_off =
        List.fold_left
          (fun dst_off { Faraday.buffer; off; len } ->
            Bigstringaf.blit buffer ~src_off:off uncompressed ~dst_off ~len;
            dst_off + len)
          8
          iovecs
      in
      `Ok (dst_off - 8))
  in
  if comp && len > 2000
  then (
    try compress ~big_endian uncompressed with
    | Exit -> uncompressed)
  else uncompressed
;;

open Angstrom

let msgtyp = any_uint8 >>| msgtyp_of_int

let uint8_flag =
  any_uint8
  >>| function
  | 0 -> false
  | 1 -> true
  | _ -> invalid_arg "uint8_flag"
;;

module type ENDIAN = module type of BE

let getmod = function
  | true -> (module BE : ENDIAN)
  | false -> (module LE : ENDIAN)
;;

let hdr =
  uint8_flag
  >>= fun is_little_endian ->
  let big_endian = not is_little_endian in
  msgtyp
  >>= fun typ ->
  uint8_flag
  >>= fun compressed ->
  any_uint8
  >>= fun _ ->
  let module M = (val getmod big_endian) in
  M.any_int32 >>| fun len -> { big_endian; compressed; typ; len }
;;

let bool_atom = char '\xff' *> any_uint8 >>| fun i -> i <> 0

let guid_encoding =
  take 16
  >>| fun guid ->
  match Uuidm.of_bytes guid with
  | None -> invalid_arg "guid"
  | Some v -> v
;;

let guid_atom = char '\xfe' *> guid_encoding
let byte_atom = char '\xfc' *> any_char

let short_encoding endianness =
  let module M = (val getmod endianness) in
  M.any_int16
;;

let short_atom endianness = char '\xfb' *> short_encoding endianness

let int_encoding endianness =
  let module M = (val getmod endianness) in
  M.any_int32
;;

let int_atom endianness = char '\xfa' *> int_encoding endianness

let long_encoding endianness =
  let module M = (val getmod endianness) in
  M.any_int64
;;

let long_atom endianness = char '\xf9' *> long_encoding endianness

let real_encoding endianness =
  let module M = (val getmod endianness) in
  M.any_float
;;

let real_atom endianness = char '\xf8' *> real_encoding endianness

let float_encoding endianness =
  let module M = (val getmod endianness) in
  M.any_double
;;

let float_atom endianness = char '\xf7' *> float_encoding endianness
let char_atom = char '\xf6' *> any_char
let symbol_encoding = take_while (fun c -> c <> '\x00') <* char '\x00'
let symbol_atom = char '\xf5' *> symbol_encoding
let err_atom = char '\x80' *> symbol_encoding

let timestamp_encoding endianness =
  let module M = (val getmod endianness) in
  M.any_int64 >>| timestamp_of_int64
;;

let timestamp_atom endianness = char '\xf4' *> timestamp_encoding endianness

let month_encoding endianness =
  let module M = (val getmod endianness) in
  M.any_int32 >>| month_of_int32
;;

let month_atom endianness = char '\xf3' *> month_encoding endianness

let date_encoding endianness =
  let module M = (val getmod endianness) in
  M.any_int32 >>| date_of_int32
;;

let date_atom endianness = char '\xf2' *> date_encoding endianness

let timespan_encoding endianness =
  let module M = (val getmod endianness) in
  M.any_int64 >>| span_of_ns
;;

let timespan_atom endianness = char '\xf0' *> timespan_encoding endianness

let minute_encoding endianness =
  let module M = (val getmod endianness) in
  M.any_int32 >>| span_of_minute
;;

let minute_atom endianness = char '\xef' *> minute_encoding endianness

let second_encoding endianness =
  let module M = (val getmod endianness) in
  M.any_int32 >>| span_of_second
;;

let second_atom endianness = char '\xee' *> second_encoding endianness

let time_encoding endianness =
  let module M = (val getmod endianness) in
  M.any_int32 >>| span_of_ms
;;

let time_atom endianness = char '\xed' *> time_encoding endianness

let length big_endian =
  let module M = (val getmod big_endian) in
  M.any_int32 >>| Int32.to_int
;;

let bool_vect endianness =
  char '\x01' *> attribute
  >>= fun _ ->
  length endianness
  >>= fun len -> take len >>| fun str -> Array.init len (fun i -> str.[i] <> '\x00')
;;

let guid_vect endianness =
  char '\x02' *> attribute
  >>= fun _ ->
  length endianness
  >>= fun len ->
  lift
    (fun buf -> Array.init len (fun i -> Option.get (Uuidm.of_bytes ~pos:(i * 16) buf)))
    (take (len * 16))
;;

let char_array len = take len >>| fun str -> Array.init len (fun i -> str.[i])

let byte_vect endianness =
  char '\x04' *> attribute >>= fun _ -> length endianness >>= fun len -> char_array len
;;

let bytestring_vect endianness =
  char '\x04' *> attribute >>= fun _ -> length endianness >>= fun len -> take len
;;

let getEndian = function
  | true -> (module EndianString.BigEndian : EndianString.EndianStringSig)
  | false -> (module EndianString.LittleEndian : EndianString.EndianStringSig)
;;

let short_vect endianness =
  char '\x05' *> attribute
  >>= fun _ ->
  length endianness
  >>= fun len ->
  lift
    (fun buf ->
      Array.init len (fun i ->
        let module E = (val getEndian endianness) in
        E.get_int16 buf (i * 2)))
    (take (len * 2))
;;

let int_vect endianness =
  char '\x06' *> attribute
  >>= fun _ ->
  length endianness
  >>= fun len ->
  lift
    (fun buf ->
      Array.init len (fun i ->
        let module E = (val getEndian endianness) in
        E.get_int32 buf (i * 4)))
    (take (len * 4))
;;

let long_vect endianness =
  char '\x07' *> attribute
  >>= fun _ ->
  length endianness
  >>= fun len ->
  lift
    (fun buf ->
      Array.init len (fun i ->
        let module E = (val getEndian endianness) in
        E.get_int64 buf (i * 8)))
    (take (len * 8))
;;

let real_vect endianness =
  char '\x08' *> attribute
  >>= fun _ ->
  length endianness
  >>= fun len ->
  lift
    (fun buf ->
      Array.init len (fun i ->
        let module E = (val getEndian endianness) in
        E.get_float buf (i * 4)))
    (take (len * 4))
;;

let float_vect endianness =
  char '\x09' *> attribute
  >>= fun _ ->
  length endianness
  >>= fun len ->
  lift
    (fun buf ->
      Array.init len (fun i ->
        let module E = (val getEndian endianness) in
        E.get_double buf (i * 8)))
    (take (len * 8))
;;

let char_vect endianness =
  char '\x0a' *> attribute >>= fun _ -> length endianness >>= fun len -> char_array len
;;

let string_vect endianness =
  char '\x0a' *> attribute >>= fun _ -> length endianness >>= fun len -> take len
;;

let symbol_vect endianness =
  char '\x0b' *> attribute
  >>= fun _ ->
  length endianness >>= fun len -> lift Array.of_list (count len symbol_encoding)
;;

let timestamp_vect endianness =
  char '\x0c' *> attribute
  >>= fun _ ->
  length endianness
  >>= fun len ->
  lift
    (fun buf ->
      Array.init len (fun i ->
        let module E = (val getEndian endianness) in
        timestamp_of_int64 (E.get_int64 buf (i * 8))))
    (take (len * 8))
;;

let month_vect endianness =
  char '\x0d' *> attribute
  >>= fun _ ->
  length endianness
  >>= fun len ->
  lift
    (fun buf ->
      Array.init len (fun i ->
        let module E = (val getEndian endianness) in
        month_of_int32 (E.get_int32 buf (i * 4))))
    (take (len * 4))
;;

let date_vect endianness =
  char '\x0e' *> attribute
  >>= fun _ ->
  length endianness
  >>= fun len ->
  lift
    (fun buf ->
      Array.init len (fun i ->
        let module E = (val getEndian endianness) in
        date_of_int32 (E.get_int32 buf (i * 4))))
    (take (len * 4))
;;

let timespan_vect endianness =
  char '\x10' *> attribute
  >>= fun _ ->
  length endianness
  >>= fun len ->
  lift
    (fun buf ->
      Array.init len (fun i ->
        let module E = (val getEndian endianness) in
        span_of_ns (E.get_int64 buf (i * 8))))
    (take (len * 8))
;;

let minute_vect endianness =
  char '\x11' *> attribute
  >>= fun _ ->
  length endianness
  >>= fun len ->
  lift
    (fun buf ->
      Array.init len (fun i ->
        let module E = (val getEndian endianness) in
        span_of_minute (E.get_int32 buf (i * 4))))
    (take (len * 4))
;;

let second_vect endianness =
  char '\x12' *> attribute
  >>= fun _ ->
  length endianness
  >>= fun len ->
  lift
    (fun buf ->
      Array.init len (fun i ->
        let module E = (val getEndian endianness) in
        span_of_second (E.get_int32 buf (i * 4))))
    (take (len * 4))
;;

let time_vect endianness =
  char '\x13' *> attribute
  >>= fun _ ->
  length endianness
  >>= fun len ->
  lift
    (fun buf ->
      Array.init len (fun i ->
        let module E = (val getEndian endianness) in
        span_of_ms (E.get_int32 buf (i * 4))))
    (take (len * 4))
;;

let lambda_atom endianness =
  char '\x64' *> symbol_encoding
  >>= fun sym -> string_vect endianness >>| fun lam -> sym, lam
;;

let unaryprim_atom = char '\x65' *> any_uint8
let operator_atom = char '\x66' *> any_uint8
let over_atom = char '\x6b' *> char '\x66' *> any_uint8

let general_list big_endian elt =
  char '\x00' *> attribute
  >>= fun _ -> length big_endian >>= fun len -> lift Array.of_list (count len elt)
;;

let stream n p f =
  if n < 0 then invalid_arg "stream: n < 0";
  let rec loop = function
    | 0 -> return ()
    | n -> lift f p >>= fun () -> loop (n - 1)
  in
  loop n
;;

let general_list_stream endianness elt f =
  char '\x00' *> attribute >>= fun _ -> length endianness >>= fun len -> stream len elt f
;;

let lambda_vect endianness = general_list endianness (lambda_atom endianness)
let unaryprim_vect endianness = general_list endianness unaryprim_atom
let operator_vect endianness = general_list endianness operator_atom
let over_vect endianness = general_list endianness over_atom

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
    if !i = 0
    then (
      f := get_int8 compressed (incrpp d);
      i := 1);
    if !f land !i <> 0
    then (
      r := aa.(get_int8 compressed (incrpp d));
      set_int8 msg (incrpp s) (get_int8 msg (incrpp r));
      set_int8 msg (incrpp s) (get_int8 msg (incrpp r));
      n := get_int8 compressed (incrpp d);
      for m = 0 to !n - 1 do
        let rm = get_int8 msg (!r + m) in
        set_int8 msg (!s + m) rm
      done)
    else set_int8 msg (incrpp s) (get_int8 compressed (incrpp d));
    while !p < pred !s do
      let i1 = get_int8 msg !p in
      let i2 = get_int8 msg (succ !p) in
      aa.(i1 lxor i2) <- incrpp p
    done;
    if !f land !i <> 0
    then (
      s := !s + !n;
      p := !s);
    i := !i * 2 mod (0xffff + 1);
    if !i = 256 then i := 0
  done
;;

let rec destruct_list : type a. bool -> a w -> a Angstrom.t =
  fun big_endian w ->
  match w with
  | Tup (ww, _) -> destruct ~big_endian ww
  | Tups (h, t, _) ->
    destruct_list big_endian h >>= fun a -> destruct_list big_endian t >>| fun b -> a, b
  | Conv (_, inj, ww) ->
    destruct_list big_endian ww
    >>= fun x ->
    (try return (inj x) with
     | exn -> fail (Printexc.to_string exn))
  | _ -> assert false

and destruct : type a. ?big_endian:bool -> a w -> a Angstrom.t =
  fun ?(big_endian = Sys.big_endian) w ->
  match w with
  | Union cases ->
    choice
      (List.map
         (fun (Case { encoding; inj; _ }) ->
           destruct ~big_endian encoding
           >>= fun x ->
           try return (inj x) with
           | exn -> fail (Printexc.to_string exn))
         cases)
  | Err -> err_atom
  | List (w, _) -> general_list big_endian (destruct ~big_endian w)
  | Conv (_, inj, w) ->
    destruct ~big_endian w
    >>= fun x ->
    (try return (inj x) with
     | exn -> fail (Printexc.to_string exn))
  | Tup (w, _) -> general_list big_endian (destruct ~big_endian w) >>| fun a -> a.(0)
  | Tups (_, _, _) as t ->
    char '\x00' *> attribute
    >>= fun _ -> length big_endian >>= fun _len -> destruct_list big_endian t
  | Dict (kw, vw, _) ->
    satisfy (function
      | '\x7f' | '\x63' -> true
      | _ -> false)
    *> destruct ~big_endian kw
    >>= fun k -> destruct ~big_endian vw >>| fun v -> k, v
  | Table (vw, _) ->
    char '\x62'
    *> satisfy (function
      | '\x00' | '\x01' -> true
      | _ -> false)
    *> destruct ~big_endian (Dict (v sym, vw, false))
  | Unit -> skip_while (fun _ -> true)
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
  | Atom UnaryPrim -> unaryprim_atom
  | Atom Operator -> operator_atom
  | Atom Over -> over_atom
  | Vect (Boolean, _) -> bool_vect big_endian
  | Vect (Guid, _) -> guid_vect big_endian
  | Vect (Byte, _) -> byte_vect big_endian
  | Vect (Short, _) -> short_vect big_endian
  | Vect (Int, _) -> int_vect big_endian
  | Vect (Long, _) -> long_vect big_endian
  | Vect (Real, _) -> real_vect big_endian
  | Vect (Float, _) -> float_vect big_endian
  | Vect (Char, _) -> char_vect big_endian
  | Vect (Symbol, _) -> symbol_vect big_endian
  | Vect (Timestamp, _) -> timestamp_vect big_endian
  | Vect (Month, _) -> month_vect big_endian
  | Vect (Date, _) -> date_vect big_endian
  | Vect (Timespan, _) -> timespan_vect big_endian
  | Vect (Minute, _) -> minute_vect big_endian
  | Vect (Second, _) -> second_vect big_endian
  | Vect (Time, _) -> time_vect big_endian
  | Vect (Lambda, _) -> lambda_vect big_endian
  | Vect (UnaryPrim, _) -> unaryprim_vect big_endian
  | Vect (Operator, _) -> operator_vect big_endian
  | Vect (Over, _) -> over_vect big_endian
  | String (Byte, _) -> bytestring_vect big_endian
  | String (Char, _) -> string_vect big_endian
  | String _ -> invalid_arg "destruct: unsupported string type"
;;

let destruct_stream : ?big_endian:bool -> 'a array w -> ('a -> unit) -> unit Angstrom.t =
  fun ?(big_endian = Sys.big_endian) w f ->
  match w with
  | List (w, _) -> general_list_stream big_endian (destruct ~big_endian w) f
  | _ -> invalid_arg "destruct_stream: not a general list"
;;

let pp_print_short ppf i =
  if i = nh then Format.pp_print_string ppf "0Nh" else Format.pp_print_int ppf i
;;

let pp_print_int ppf i =
  if i = ni then Format.pp_print_string ppf "0Ni" else Format.fprintf ppf "%ld" i
;;

let pp_print_long ppf i =
  if i = nj then Format.pp_print_string ppf "0Nj" else Format.fprintf ppf "%Ld" i
;;

let pp_print_real ppf i =
  match Float.classify_float i with
  | FP_nan -> Format.pp_print_string ppf "0Ne"
  | FP_infinite when i > 0. -> Format.pp_print_string ppf "0We"
  | FP_infinite -> Format.pp_print_string ppf "0We"
  | _ -> Format.fprintf ppf "%g" i
;;

let pp_print_float ppf i =
  match Float.classify_float i with
  | FP_nan -> Format.pp_print_string ppf "0Nf"
  | FP_infinite when i > 0. -> Format.pp_print_string ppf "0Wf"
  | FP_infinite -> Format.pp_print_string ppf "0Wf"
  | _ -> Format.fprintf ppf "%g" i
;;

let rec pp_print_list : type a. Format.formatter -> a w -> a -> unit =
  fun ppf w v ->
  match w with
  | Tup (ww, _) -> pp ww ppf v
  | Tups (h, t, _) ->
    pp_print_list ppf h (fst v);
    Format.pp_print_char ppf ' ';
    pp_print_list ppf t (snd v)
  | Conv (proj, _, w) -> pp_print_list ppf w (proj v)
  | _ -> assert false

and pp : type a. a w -> Format.formatter -> a -> unit =
  fun w ppf v ->
  let pp_sep ppf () = Format.pp_print_char ppf ' ' in
  let pp_sep_empty ppf () = Format.pp_print_string ppf "" in
  match w with
  | Unit -> Format.pp_print_string ppf "(::)"
  | List (w, _) -> Format.(pp_print_list ~pp_sep (pp w) ppf (Array.to_list v))
  | Conv (proj, _, w) -> pp w ppf (proj v)
  | Tup (w, _) -> pp w ppf v
  | Tups _ -> pp_print_list ppf w v
  | Dict (kw, vw, _sorted) -> Format.fprintf ppf "%a!%a" (pp kw) (fst v) (pp vw) (snd v)
  | Table (vw, _sorted) ->
    Format.fprintf ppf "+%a!%a" (pp (Vect (Symbol, None))) (fst v) (pp vw) (snd v)
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
  | Atom Timespan -> Ptime.Span.pp ppf v
  | Atom Minute -> Ptime.Span.pp ppf v
  | Atom Second -> Ptime.Span.pp ppf v
  | Atom Time -> Ptime.Span.pp ppf v
  | Atom Lambda -> pp_print_lambda ppf v
  | Atom UnaryPrim -> pp_print_unaryprim ppf v
  | Atom Operator -> pp_print_operator ppf v
  | Atom Over -> pp_print_over ppf v
  | Vect (Boolean, _) ->
    Format.pp_print_list ~pp_sep Format.pp_print_bool ppf (Array.to_list v)
  | Vect (Guid, _) -> Format.pp_print_list ~pp_sep Uuidm.pp ppf (Array.to_list v)
  | Vect (Byte, _) ->
    Format.pp_print_list
      ~pp_sep
      (fun ppf -> Format.fprintf ppf "X%c")
      ppf
      (Array.to_list v)
  | Vect (Short, _) -> Format.pp_print_list ~pp_sep pp_print_short ppf (Array.to_list v)
  | Vect (Int, _) -> Format.pp_print_list ~pp_sep pp_print_int ppf (Array.to_list v)
  | Vect (Long, _) -> Format.pp_print_list ~pp_sep pp_print_long ppf (Array.to_list v)
  | Vect (Real, _) -> Format.pp_print_list ~pp_sep pp_print_real ppf (Array.to_list v)
  | Vect (Float, _) -> Format.pp_print_list ~pp_sep pp_print_float ppf (Array.to_list v)
  | Vect (Char, _) ->
    Format.pp_print_string ppf (String.init (Array.length v) (Array.get v))
  | Vect (Symbol, _) ->
    Format.pp_print_list
      ~pp_sep:pp_sep_empty
      (fun ppf -> Format.fprintf ppf "`%s")
      ppf
      (Array.to_list v)
  | Vect (Timestamp, _) ->
    Format.pp_print_list ~pp_sep pp_print_timestamp ppf (Array.to_list v)
  | Vect (Month, _) -> Format.pp_print_list ~pp_sep pp_print_month ppf (Array.to_list v)
  | Vect (Date, _) -> Format.pp_print_list ~pp_sep pp_print_date ppf (Array.to_list v)
  | Vect (Timespan, _) -> Format.pp_print_list ~pp_sep Ptime.Span.pp ppf (Array.to_list v)
  | Vect (Minute, _) -> Format.pp_print_list ~pp_sep Ptime.Span.pp ppf (Array.to_list v)
  | Vect (Second, _) -> Format.pp_print_list ~pp_sep Ptime.Span.pp ppf (Array.to_list v)
  | Vect (Time, _) -> Format.pp_print_list ~pp_sep Ptime.Span.pp ppf (Array.to_list v)
  | Vect (Lambda, _) -> Format.pp_print_list ~pp_sep pp_print_lambda ppf (Array.to_list v)
  | Vect (UnaryPrim, _) ->
    Format.pp_print_list ~pp_sep pp_print_unaryprim ppf (Array.to_list v)
  | Vect (Operator, _) ->
    Format.pp_print_list ~pp_sep pp_print_operator ppf (Array.to_list v)
  | Vect (Over, _) -> Format.pp_print_list ~pp_sep pp_print_over ppf (Array.to_list v)
  | String (Byte, _) -> Format.pp_print_string ppf v
  | String (Char, _) -> Format.pp_print_string ppf v
  | String _ -> invalid_arg "pp: unsupported string type"
  | Err -> Format.pp_print_string ppf v
  | Union _ -> Format.pp_print_string ppf "<union>"
;;

type t =
  | K :
      { big_endian : bool
      ; typ : msgtyp
      ; w : 'a w
      ; msg : 'a
      }
      -> t

let big_endian (K { big_endian; _ }) = big_endian
let typ (K { typ; _ }) = typ

let create ?(typ = Async) ?(big_endian = Sys.big_endian) w msg =
  K { big_endian; typ; w; msg }
;;

let pp_serialized ppf (K { big_endian; typ; w; msg }) =
  let serialized = construct_bigstring ~big_endian ~typ w msg in
  Format.fprintf ppf "0x%a" Hex.pp (Hex.of_bigstring serialized)
;;

let pp_hum ppf (K { big_endian; typ; w; msg }) =
  Format.fprintf
    ppf
    "%b %a @[%a@]"
    big_endian
    Sexplib.Sexp.pp
    (sexp_of_msgtyp typ)
    (pp w)
    msg
;;

let construct_msg buf (K { big_endian; w; msg; _ }) =
  let module FE =
    (val Faraday.(if big_endian then (module BE : FE) else (module LE : FE)))
  in
  construct (module FE) buf w msg
;;

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
