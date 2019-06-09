(*---------------------------------------------------------------------------
   Copyright (c) 2018 Vincent Bernardoff. All rights reserved.
   Distributed under the ISC license, see terms at the end of the file.
  ---------------------------------------------------------------------------*)

let nh = 0xffff_8000
let wh = 0x7fff

let ni = 0x8000_0000l
let wi = 0x7fff_ffffl

let nj = 0x8000_0000_0000_0000L
let wj = 0x7fff_ffff_ffff_ffffL

let nf = nan
let wf = infinity

type time = { time : Ptime.time ; ms : int }
type timespan = { time : Ptime.time ; ns : int }

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
  | Timespan : timespan typ
  | Minute : Ptime.time typ
  | Second : Ptime.time typ
  | Time : time typ
  | Lambda : (string * string) typ

(* let int_of_typ : type a. a typ -> int = function
 *   | Boolean -> 1
 *   | Guid -> 2
 *   | Byte -> 3
 *   | Short -> 5
 *   | Int -> 6
 *   | Long -> 7
 *   | Real -> 8
 *   | Float -> 9
 *   | Char -> 10
 *   | Symbol -> 11
 *   | Timestamp -> 12
 *   | Month -> 13
 *   | Date -> 14
 *   | Timespan -> 15
 *   | Minute -> 17
 *   | Second -> 18
 *   | Time -> 19 *)

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
  | NoAttr
  | Sorted
  | Unique
  | Parted
  | Grouped

let char_of_attribute = function
  | NoAttr -> '\x00'
  | Sorted -> '\x01'
  | Unique -> '\x02'
  | Parted -> '\x03'
  | Grouped -> '\x04'

let attribute attr =
  Angstrom.(char (char_of_attribute attr) >>| ignore)

type _ w =
  | Atom : 'a typ -> 'a w
  | Vect : 'a typ * attribute -> 'a list w
  | String : char typ * attribute -> string w
  | List : 'a w * attribute -> 'a list w
  | Tup : 'a w * attribute -> 'a w
  | Tups : 'a w * 'b w * attribute -> ('a * 'b) w
  | Dict : 'a w * 'b w * bool -> ('a * 'b) w
  | Table : 'a w * 'b w * bool -> ('a * 'b) w
  | Conv : ('a -> 'b) * ('b -> 'a) * 'b w -> 'a w

let is_list_type : type a. a w -> bool = function
  | Tup _ -> true
  | Tups _ -> true
  | List _ -> true
  | Vect _ -> true
  | _ -> false

let parted : type a. a w -> a w = function
  | Tup (a, _) -> Tup (a, Parted)
  | Tups (a, b, _) -> Tups (a, b, Parted)
  | List (a, _) -> List (a, Parted)
  | Vect (a, _) -> Vect (a, Parted)
  | _ -> invalid_arg "parted"

(* let rec attr : type a. a w -> attribute option = function
 *   | Vect (_, attr) -> Some attr
 *   | String (_, attr) -> Some attr
 *   | List (_, attr) -> Some attr
 *   | Conv (_, _, a) -> attr a
 *   | Tup (_, attr) -> Some attr
 *   | Tups (_, _, attr) -> Some attr
 *   | _ -> None
 * 
 * let rec int_of_w : type a. a w -> int = function
 *   | Atom a -> -(int_of_typ a)
 *   | Vect (a, _) -> int_of_typ a
 *   | String (a, _) -> int_of_typ a
 *   | List _ -> 0
 *   | Tup _ -> 0
 *   | Tups _ -> 0
 *   | Dict _ -> 99
 *   | Table _ -> 98
 *   | Conv (_, _, a) -> int_of_w a *)

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
  | _ -> false

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
  | _ -> false

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

let a a = Atom a
let v ?(attr=NoAttr) a = Vect (a, attr)
let s ?(attr=NoAttr) a = String (a, attr)

let list ?(attr=NoAttr) a = List (a, attr)

let tup a attr = Tup (a, attr)
let tups a b attr = Tups (a, b, attr)

let t1 ?(attr=NoAttr) a = tup a attr
let t2 ?(attr=NoAttr) a b = tups (tup a attr) (tup b attr) attr
let t3 ?(attr=NoAttr) a b c =
  conv
    (fun (a, b, c) -> (a, (b, c)))
    (fun (a, (b, c)) -> (a, b, c))
    (tups (tup a attr) (t2 ~attr b c) attr)

let t4 ?(attr=NoAttr) a b c d =
  conv
    (fun (a, b, c, d) -> (a, (b, c, d)))
    (fun (a, (b, c, d)) -> (a, b, c, d))
    (tups (tup a attr) (t3 ~attr b c d) attr)

let t5 ?(attr=NoAttr) a b c d e =
  conv
    (fun (a, b, c, d, e) -> (a, (b, c, d, e)))
    (fun (a, (b, c, d, e)) -> (a, b, c, d, e))
    (tups (tup a attr) (t4 ~attr b c d e) attr)

let t6 ?(attr=NoAttr) a b c d e f =
  conv
    (fun (a, b, c, d, e, f) -> (a, (b, c, d, e, f)))
    (fun (a, (b, c, d, e, f)) -> (a, b, c, d, e, f))
    (tups (tup a attr) (t5 ~attr b c d e f) attr)

let t7 ?(attr=NoAttr) a b c d e f g =
  conv
    (fun (a, b, c, d, e, f, g) -> (a, (b, c, d, e, f, g)))
    (fun (a, (b, c, d, e, f, g)) -> (a, b, c, d, e, f, g))
    (tups (tup a attr) (t6 ~attr b c d e f g) attr)

let t8 ?(attr=NoAttr) a b c d e f g h =
  conv
    (fun (a, b, c, d, e, f, g, h) -> (a, (b, c, d, e, f, g, h)))
    (fun (a, (b, c, d, e, f, g, h)) -> (a, b, c, d, e, f, g, h))
    (tups (tup a attr) (t7 ~attr b c d e f g h) attr)

let t9 ?(attr=NoAttr) a b c d e f g h i =
  conv
    (fun (a, b, c, d, e, f, g, h, i) -> (a, (b, c, d, e, f, g, h, i)))
    (fun (a, (b, c, d, e, f, g, h, i)) -> (a, b, c, d, e, f, g, h, i))
    (tups (tup a attr) (t8 ~attr b c d e f g h i) attr)

let t10 ?(attr=NoAttr) a b c d e f g h i j =
  conv
    (fun (a, b, c, d, e, f, g, h, i, j) -> (a, (b, c, d, e, f, g, h, i, j)))
    (fun (a, (b, c, d, e, f, g, h, i, j)) -> (a, b, c, d, e, f, g, h, i, j))
    (tups (tup a attr) (t9 ~attr b c d e f g h i j) attr)

let merge_tups t1 t2 =
  let rec is_tup : type t. t w -> attribute option = function
    | Tup (_, a) -> Some a
    | Tups (_, _, a)(* by construction *) -> Some a
    | Conv (_, _, t) -> is_tup t
    | _ -> None in
  match is_tup t1, is_tup t2 with
  | Some a, Some b when a = b -> Tups (t1, t2, a)
  | _ -> invalid_arg "merge_tups"

let dict ?(sorted=false) k v = Dict (k, v, sorted)
let table ?(sorted=false) k v = Table (k, v, sorted)

let millenium =
  match Ptime.of_date (2000, 1, 1) with
  | None -> assert false
  | Some t -> t

let of_day_exn d =
  match Ptime.Span.of_d_ps (d, 0L) with
  | None -> invalid_arg "Ptime.Span.of_d_ps"
  | Some t -> t

let add_span_exn t s =
  match Ptime.add_span t s with
  | None -> invalid_arg "Ptime.add_span"
  | Some t -> t

let date_of_int d =
  Ptime.to_date (add_span_exn millenium (of_day_exn d))

let int_of_date d =
  match Ptime.of_date d with
  | None -> invalid_arg "int_of_date"
  | Some t ->
    fst Ptime.(Span.to_d_ps (diff t millenium))

let int_of_time { time = ((h, m, s), tz_offset) ; ms } =
  (h - tz_offset) * 3600 + m * 60 + s * 1_000 + ms

let time_of_int time =
  let ms = time mod 1_000 in
  let s = time / 1_000 in
  let hh = s / 3600 in
  let mm = (s / 60) mod 60 in
  let ss = s mod 60 in
  { time = ((hh, mm, ss), 0); ms }

let int64_of_timespan { time = ((h, m, s), tz_offset) ; ns } =
  let open Int64 in
  add
    (mul (of_int ((h - tz_offset) * 3600 + m * 60 + s)) 1_000_000_000L)
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
  | ts when Ptime.(equal ts min) -> Int64.min_int
  | ts ->
    let span_since_kxepoch =
      Ptime.(Span.sub (to_span ts) kx_epoch_span) in
    let d, ps = Ptime.Span.to_d_ps span_since_kxepoch in
    Int64.(add (day_in_ns d) (div ps 1_000L))

let timestamp_of_int64 = function
  | i when Int64.(equal i min_int) -> Ptime.min
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

let int_of_month (y, m, _) = (y - 2000) * 12 + (pred m)
let month_of_int m =
  let y = m / 12 in
  let rem_m = m mod 12 in
  2000 + y, succ rem_m, 0

let int_of_minute ((hh, mm, _), tz_offset) = (hh * 60 + mm + tz_offset / 60)

let minute_of_int nb_minutes =
  (nb_minutes / 60, nb_minutes mod 60, 0), 0

let int_of_second ((hh, mm, ss), tz_offset) =
  (hh * 3600 + mm * 60 + ss + tz_offset)

let second_of_int nb_seconds =
  let hh = nb_seconds / 3600 in
  let mm = (nb_seconds / 60) mod 60 in
  let ss = nb_seconds mod 60 in
  (hh, mm, ss), 0

(* let string_of_chars a = String.init (Array.length a) (Array.get a) *)
let pp_print_month ppf (y, m, _) = Format.fprintf ppf "%d.%dm" y m
let pp_print_date ppf (y, m, d) =  Format.fprintf ppf "%d.%d.%d" y m d
let pp_print_timespan ppf { time = ((hh, mm, ss), _) ; ns } = Format.fprintf ppf "%d:%d:%d.%d" hh mm ss ns
let pp_print_minute ppf ((hh, mm, _), _) = Format.fprintf ppf "%d:%d" hh mm
let pp_print_second ppf ((hh, mm, ss), _) = Format.fprintf ppf "%d:%d:%d" hh mm ss
let pp_print_time ppf { time = ((hh, mm, ss), _) ; ms } = Format.fprintf ppf "%d:%d:%d.%d" hh mm ss ms
(* let pp_print_symbols ppf syms = Array.iter (fun sym -> Format.fprintf ppf "`%s" sym) syms *)
let pp_print_timestamp ppf ts = Format.fprintf ppf "%a" (Ptime.pp_rfc3339 ~frac_s:9 ()) ts

module type FE = module type of Faraday.BE

let rec construct_list :
  type a. [`Big | `Little] -> Faraday.t -> a w -> a -> int = fun e buf w a ->
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

and construct : type a. [`Big | `Little] -> Faraday.t -> a w -> a -> unit = fun e buf w a ->
  let open Faraday in
  let module FE = (val (match e with `Big -> (module BE) | `Little -> (module LE)) : FE) in
  match w with
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
    if not (is_list_type k && is_list_type v) then
      invalid_arg "dict keys and values must be a list type" ;
    let x, y = a in
    write_char buf (if sorted then '\x7f' else '\x63') ;
    construct e buf k x ;
    construct e buf v y

  | Table (k, v, sorted) ->
    write_char buf '\x62' ;
    write_char buf (if sorted then '\x01' else '\x00') ;
    construct e buf (Dict (k, (if sorted then (parted v) else v), sorted)) a

  | Conv (project, _, ww) -> construct e buf ww (project a)

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
    FE.write_uint32 buf (Int32.of_int (int_of_month a))

  | Atom Date ->
    write_char buf '\xf2' ;
    FE.write_uint32 buf (Int32.of_int (int_of_date a))

  | Atom Timespan ->
    write_char buf '\xf0' ;
    FE.write_uint64 buf (int64_of_timespan a)

  | Atom Minute ->
    write_char buf '\xef' ;
    FE.write_uint32 buf (Int32.of_int (int_of_minute a))

  | Atom Second ->
    write_char buf '\xee' ;
    FE.write_uint32 buf (Int32.of_int (int_of_second a))

  | Atom Time ->
    write_char buf '\xed' ;
    FE.write_uint32 buf (Int32.of_int (int_of_time a))

  | Atom Lambda ->
    write_char buf '\x64' ;
    write_string buf (fst a) ;
    write_char buf '\x00' ;
    construct e buf (String (Char, NoAttr)) (snd a)

  | Vect (Lambda, attr) ->
    begin match attr with
    | NoAttr
    | Grouped -> ()
    | _ -> invalid_arg "lambda cannot have attr except grouped"
    end ;
    write_char buf '\x00' ;
    write_char buf (char_of_attribute attr) ;
    FE.write_uint32 buf (Int32.of_int (List.length a)) ;
    List.iter (fun lam -> construct e buf (Atom Lambda) lam) a

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
      FE.write_uint32 buf (Int32.of_int (int_of_month m))
    end a

  | Vect (Date, attr) ->
    write_char buf '\x0e' ;
    write_char buf (char_of_attribute attr) ;
    FE.write_uint32 buf (Int32.of_int (List.length a)) ;
    List.iter begin fun m ->
      FE.write_uint32 buf (Int32.of_int (int_of_date m))
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
      FE.write_uint32 buf (Int32.of_int (int_of_minute m))
    end a

  | Vect (Second, attr) ->
    write_char buf '\x12' ;
    write_char buf (char_of_attribute attr) ;
    FE.write_uint32 buf (Int32.of_int (List.length a)) ;
    List.iter begin fun m ->
      FE.write_uint32 buf (Int32.of_int (int_of_second m))
    end a

  | Vect (Time, attr) ->
    write_char buf '\x13' ;
    write_char buf (char_of_attribute attr) ;
    FE.write_uint32 buf (Int32.of_int (List.length a)) ;
    List.iter begin fun a ->
      FE.write_uint32 buf (Int32.of_int (int_of_time a))
    end a

  | String _ -> assert false

let int_of_endianness = function
  | `Big -> 0
  | `Little -> 1

let int_of_msgtyp = function
  | `Async -> 0
  | `Sync -> 1
  | `Response -> 2

type hdr = {
  endianness: [`Little | `Big] ;
  typ: [`Async | `Sync | `Response] ;
  len: int32 ;
}

let write_hdr buf { endianness; typ; len } =
  let open Faraday in
  let module FE = (val (match endianness with
      | `Big -> (module BE) | `Little -> (module LE)) : FE) in
  write_uint8 buf (int_of_endianness endianness) ;
  write_uint8 buf (int_of_msgtyp typ) ;
  FE.write_uint16 buf 0 ;
  FE.write_uint32 buf len

let construct
    ?(endianness=if Sys.big_endian then `Big else `Little)
    ?(typ=`Async) ~hdr ~payload w x =
  let open Faraday in
  let module FE = (val (match endianness with
      | `Big -> (module BE) | `Little -> (module LE)) : FE) in
  construct endianness payload w x ;
  write_uint8 hdr (int_of_endianness endianness) ;
  write_uint8 hdr (int_of_msgtyp typ) ;
  FE.write_uint16 hdr 0 ;
  FE.write_uint32 hdr (Int32.of_int (8 + pending_bytes payload))

let msgtyp_of_int = function
  | 0 -> `Async
  | 1 -> `Sync
  | 3 -> `Response
  | _ -> invalid_arg "typ_of_int"

open Angstrom

let msgtyp = any_uint8 >>| msgtyp_of_int

let endianness =
  any_uint8 >>| function
  | 0 -> `Big
  | 1 -> `Little
  | _ -> invalid_arg "endianness"

module type ENDIAN = module type of BE

let getmod = function
  | `Big -> (module BE : ENDIAN)
  | `Little -> (module LE : ENDIAN)

let hdr_encoding =
  endianness >>= fun e ->
  msgtyp >>= fun typ ->
  let module M = (val getmod e) in
  M.int16 0 *>
  M.any_int32 >>| fun len ->
  { endianness = e ; typ ; len }

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
  let open Angstrom in
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
  take_while (fun c -> c <> '\x00') >>= fun res ->
  char '\x00' >>| fun _ -> res

let symbol_atom =
  char '\xf5' *> symbol_encoding

let timestamp_encoding endianness =
  let module M = (val getmod endianness) in
  M.any_int64 >>| timestamp_of_int64

let timestamp_atom endianness =
  char '\xf4' *> timestamp_encoding endianness

let month_encoding endianness =
  let module M = (val getmod endianness) in
  M.any_int32 >>| fun i -> month_of_int (Int32.to_int i)

let month_atom endianness =
  char '\xf3' *> month_encoding endianness

let date_encoding endianness =
  let module M = (val getmod endianness) in
  M.any_int32 >>| fun i -> date_of_int (Int32.to_int i)

let date_atom endianness =
  char '\xf2' *> date_encoding endianness

let timespan_encoding endianness =
  let module M = (val getmod endianness) in
  M.any_int64 >>| timespan_of_int64

let timespan_atom endianness =
  char '\xf0' *> timespan_encoding endianness

let minute_encoding endianness =
  let module M = (val getmod endianness) in
  M.any_int32 >>| fun i -> minute_of_int (Int32.to_int i)

let minute_atom endianness =
  char '\xef' *> minute_encoding endianness

let second_encoding endianness =
  let module M = (val getmod endianness) in
  M.any_int32 >>| fun i -> second_of_int (Int32.to_int i)

let second_atom endianness =
  char '\xee' *> second_encoding endianness

let time_encoding endianness =
  let module M = (val getmod endianness) in
  M.any_int32 >>| fun i -> time_of_int (Int32.to_int i)

let time_atom endianness =
  char '\xed' *> time_encoding endianness

let length endianness =
  let module M = (val getmod endianness) in
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
  string_vect endianness NoAttr >>| fun lam ->
  (sym, lam)

let general_list endianness attr elt =
  let open Angstrom in
  char '\x00' *>
  attribute attr >>= fun () ->
  length endianness >>= fun len ->
  count len elt

let lambda_vect endianness attr =
  general_list endianness attr (lambda_atom endianness)

let rec destruct_list :
  type a. [`Big | `Little] -> a w -> a Angstrom.t = fun endianness w ->
  let open Angstrom in
  match w with
  | Tup (ww, _) -> destruct ~endianness ww
  | Tups (h, t, _) ->
    destruct_list endianness h >>= fun a ->
    destruct_list endianness t >>| fun b ->
    (a, b)
  | _ -> assert false

and destruct :
  type a. ?endianness:[`Big | `Little] -> a w -> a Angstrom.t =
  fun ?(endianness = if Sys.big_endian then `Big else `Little) w ->
  let open Angstrom in
  match w with
  | List (w, attr) -> general_list endianness attr (destruct ~endianness w)
  | Conv (_, inject, w) -> destruct ~endianness w >>| inject
  | Tup (w, attr) -> general_list endianness attr (destruct ~endianness w) >>| List.hd
  | Tups (_, _, attr) as t ->
    char '\x00' *>
    attribute attr >>= fun () ->
    length endianness >>= fun _len ->
    destruct_list endianness t

  | Dict (kw, vw, sorted) ->
    char (if sorted then '\x7f' else '\x63') *>
    destruct ~endianness kw >>= fun k ->
    destruct ~endianness vw >>| fun v ->
    k, v

  | Table (kw, vw, sorted) ->
    char '\x62' *>
    char (if sorted then '\x01' else '\x00') *>
    destruct ~endianness (Dict (kw, vw, false))

  | Atom Boolean -> bool_atom
  | Atom Guid -> guid_atom
  | Atom Byte -> byte_atom
  | Atom Short -> short_atom endianness
  | Atom Int -> int_atom endianness
  | Atom Long -> long_atom endianness
  | Atom Real -> real_atom endianness
  | Atom Float -> float_atom endianness
  | Atom Char -> char_atom
  | Atom Symbol -> symbol_atom
  | Atom Timestamp -> timestamp_atom endianness
  | Atom Month -> month_atom endianness
  | Atom Date -> date_atom endianness
  | Atom Timespan -> timespan_atom endianness
  | Atom Minute -> minute_atom endianness
  | Atom Second -> second_atom endianness
  | Atom Time -> time_atom endianness
  | Atom Lambda -> lambda_atom endianness

  | Vect (Boolean, attr) -> bool_vect endianness attr
  | Vect (Guid, attr) -> guid_vect endianness attr
  | Vect (Byte, attr) -> byte_vect endianness attr
  | String (Byte, attr) -> bytestring_vect endianness attr
  | Vect (Short, attr) -> short_vect endianness attr
  | Vect (Int, attr)  -> int_vect endianness attr
  | Vect (Long, attr) -> long_vect endianness attr
  | Vect (Real, attr) -> real_vect endianness attr
  | Vect (Float, attr) -> float_vect endianness attr
  | Vect (Char, attr) -> char_vect endianness attr
  | String (Char, attr) -> string_vect endianness attr
  | Vect (Symbol, attr) -> symbol_vect endianness attr
  | Vect (Timestamp, attr) -> timestamp_vect endianness attr
  | Vect (Month, attr) -> month_vect endianness attr
  | Vect (Date, attr) -> date_vect endianness attr
  | Vect (Timespan, attr) -> timespan_vect endianness attr
  | Vect (Minute, attr) -> minute_vect endianness attr
  | Vect (Second, attr) -> second_vect endianness attr
  | Vect (Time, attr) -> time_vect endianness attr
  | Vect (Lambda, attr) -> lambda_vect endianness attr

  | _ -> invalid_arg "destruct"

let destruct ?endianness v =
  hdr_encoding >>= fun hdr ->
  destruct ?endianness v >>| fun v ->
  hdr, v

let rec pp_print_list :
  type a. Format.formatter -> a w -> a -> unit = fun ppf w v ->
  match w with
  | Tup (ww, _) -> pp ww ppf v
  | Tups (h, t, _) ->
    pp_print_list ppf h (fst v) ;
    Format.pp_print_newline ppf () ;
    pp_print_list ppf t (snd v)
  | Conv (project, _, w) ->
    pp_print_list ppf w (project v)
  | _ -> assert false

and pp :
  type a. a w -> Format.formatter -> a -> unit = fun w ppf v ->
  let pp_sep ppf () = Format.pp_print_char ppf ' ' in
  match w with
  | List (w, _) -> Format.(pp_print_list ~pp_sep:pp_print_newline (pp w) ppf v)
  | Conv (project, _, w) -> pp w ppf (project v)
  | Tup (w, _) -> pp w ppf v
  | Tups _ -> pp_print_list ppf w v
  | Dict (kw, vw, _sorted) -> Format.fprintf ppf "(%a)!(%a)" (pp kw) (fst v) (pp vw) (snd v)
  | Table (kw, vw, _sorted) -> Format.fprintf ppf "(flip (%a)!(%a))" (pp kw) (fst v) (pp vw) (snd v)

  | Atom Boolean -> Format.pp_print_bool ppf v
  | Atom Guid -> Uuidm.pp ppf v
  | Atom Byte -> Format.fprintf ppf "X%c" v
  | Atom Short -> Format.pp_print_int ppf v
  | Atom Int -> Format.fprintf ppf "%ld" v
  | Atom Long -> Format.fprintf ppf "%Ld" v
  | Atom Real -> Format.fprintf ppf "%g" v
  | Atom Float -> Format.fprintf ppf "%g" v
  | Atom Char -> Format.pp_print_char ppf v
  | Atom Symbol -> Format.fprintf ppf "`%s" v
  | Atom Timestamp -> pp_print_timestamp ppf v
  | Atom Month -> pp_print_month ppf v
  | Atom Date -> pp_print_date ppf v
  | Atom Timespan -> pp_print_timespan ppf v
  | Atom Minute -> pp_print_minute ppf v
  | Atom Second -> pp_print_second ppf v
  | Atom Time -> pp_print_time ppf v

  | Vect (Boolean, _) -> Format.pp_print_list ~pp_sep Format.pp_print_bool ppf v
  | Vect (Guid, _) -> Format.pp_print_list ~pp_sep Uuidm.pp ppf v
  | Vect (Byte, _) -> Format.pp_print_list ~pp_sep (fun ppf -> Format.fprintf ppf "X%c") ppf v
  | String (Byte, _) -> Format.pp_print_string ppf v
  | Vect (Short, _) ->Format.pp_print_list ~pp_sep Format.pp_print_int ppf v
  | Vect (Int, _)  -> Format.pp_print_list ~pp_sep (fun ppf -> Format.fprintf ppf "%ld") ppf v
  | Vect (Long, _) -> Format.pp_print_list ~pp_sep (fun ppf -> Format.fprintf ppf "%Ld") ppf v
  | Vect (Real, _) -> Format.pp_print_list ~pp_sep (fun ppf -> Format.fprintf ppf "%g") ppf v
  | Vect (Float, _) -> Format.pp_print_list ~pp_sep (fun ppf -> Format.fprintf ppf "%g") ppf v
  | Vect (Char, _) -> Format.pp_print_string ppf (String.init (List.length v) (List.nth v))
  | String (Char, _) -> Format.pp_print_string ppf v
  | Vect (Symbol, _) -> Format.pp_print_list ~pp_sep Format.pp_print_string ppf v
  | Vect (Timestamp, _) -> Format.pp_print_list ~pp_sep pp_print_timestamp ppf v
  | Vect (Month, _) -> Format.pp_print_list ~pp_sep pp_print_month ppf v
  | Vect (Date, _) -> Format.pp_print_list ~pp_sep pp_print_date ppf v
  | Vect (Timespan, _) -> Format.pp_print_list ~pp_sep pp_print_timespan ppf v
  | Vect (Minute, _) -> Format.pp_print_list ~pp_sep pp_print_minute ppf v
  | Vect (Second, _) -> Format.pp_print_list ~pp_sep pp_print_second ppf v
  | Vect (Time, _) -> Format.pp_print_list ~pp_sep pp_print_time ppf v

  | _ -> invalid_arg "destruct"


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
