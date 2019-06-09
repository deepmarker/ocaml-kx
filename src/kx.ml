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

open Bigarray

type ('ml, 'c) bv = ('ml, 'c, c_layout) Array1.t

type gv = (int, int8_unsigned_elt) bv
type hv = (int, int16_signed_elt)  bv
type iv = (int32, int32_elt)       bv
type jv = (int64, int64_elt)       bv
type ev = (float, float32_elt)     bv
type fv = (float, float64_elt)     bv

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

let int_of_typ : type a. a typ -> int = function
  | Boolean -> 1
  | Guid -> 2
  | Byte -> 3
  | Short -> 5
  | Int -> 6
  | Long -> 7
  | Real -> 8
  | Float -> 9
  | Char -> 10
  | Symbol -> 11
  | Timestamp -> 12
  | Month -> 13
  | Date -> 14
  | Timespan -> 15
  | Minute -> 17
  | Second -> 18
  | Time -> 19

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
  | _ -> None

let eq_typ_val : type a b. a typ -> b typ -> a -> b -> (a, b) eq option = fun a b x y ->
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
  | _ -> None

type attribute =
  | NoAttr
  | Sorted
  | Unique
  | Parted
  | Grouped

let int_of_attribute = function
  | NoAttr -> 0
  | Sorted -> 1
  | Unique -> 2
  | Parted -> 3
  | Grouped -> 4

let char_of_attribute = function
  | NoAttr -> '\x00'
  | Sorted -> '\x01'
  | Unique -> '\x02'
  | Parted -> '\x03'
  | Grouped -> '\x04'

type _ w =
  | Atom : 'a typ -> 'a w
  | Vect : 'a typ * attribute -> 'a array w
  | String : char typ * attribute -> string w
  | List : 'a w * attribute -> 'a array w
  | Tup : 'a w * attribute -> 'a w
  | Tups : 'a w * 'b w * attribute -> ('a * 'b) w
  | Dict : 'a w * 'b w -> ('a * 'b) w
  | Table : 'a w * 'b w -> ('a * 'b) w
  | Conv : ('a -> 'b) * ('b -> 'a) * 'b w -> 'a w

let rec attr : type a. a w -> attribute option = function
  | Vect (_, attr) -> Some attr
  | String (_, attr) -> Some attr
  | List (_, attr) -> Some attr
  | Conv (_, _, a) -> attr a
  | Tup (_, attr) -> Some attr
  | Tups (_, _, attr) -> Some attr
  | _ -> None

type t = K : 'a w * 'a * 'a Fmt.t -> t

let rec int_of_w : type a. a w -> int = function
  | Atom a -> -(int_of_typ a)
  | Vect (a, _) -> int_of_typ a
  | String (a, _) -> int_of_typ a
  | List _ -> 0
  | Tup _ -> 0
  | Tups _ -> 0
  | Dict _ -> 99
  | Table _ -> 98
  | Conv (_, _, a) -> int_of_w a

let rec equal : type a b. a w -> b w -> bool = fun a b ->
  match a, b with
  | Atom a, Atom b -> eq_typ a b <> None
  | Vect (a, aa), Vect (b, ba) -> eq_typ a b <> None && aa = ba
  | String (a, aa), String (b, ba) -> eq_typ a b <> None && aa = ba
  | List (a, aa), List (b, ba) -> equal a b && aa = ba
  | Tup (a, aa), Tup (b, ba) -> equal a b && aa = ba
  | Tups (a, b, aa), Tups (c, d, ba) -> equal a c && equal b d && aa = ba
  | Dict (a, b), Dict (c, d) -> equal a c && equal b d
  | Table (a, b), Table (c, d) -> equal a c && equal b d
  | Conv (_, _, a), Conv (_, _, b) -> equal a b
  | _ -> false

let rec equal_typ_val : type a b. a w -> b w -> a -> b -> bool = fun a b x y ->
  match a, b with
  | Atom a, Atom b -> eq_typ_val a b x y  <> None
  | Vect (a, aa), Vect (b, ba) -> begin
      aa = ba &&
      match Array.length x, Array.length y with
      | a, b when a <> b -> false
      | len, _ ->
        let ret = ref true in
        for i = 0 to len - 1 do
          ret := !ret && eq_typ_val a b x.(i) y.(i) <> None
        done ;
        !ret
    end
  | String _, String _ -> String.equal x y
  | List (a, aa), List (b, ba) ->
    aa = ba &&
    begin
      match Array.length x, Array.length y with
      | a, b when a <> b -> false
      | len, _ ->
        let ret = ref true in
        for i = 0 to len - 1 do
          ret := !ret && equal_typ_val a b x.(i) y.(i)
        done ;
        !ret
    end
  | Tup (a, aa), Tup (b, ba) -> aa = ba && equal_typ_val a b x y
  | Tups (a, b, aa), Tups (c, d, ba) ->
    let x1, x2 = x in
    let y1, y2 = y in
    aa = ba && equal_typ_val a c x1 y1 && equal_typ_val b d x2 y2
  | Dict (a, b), Dict (c, d) ->
    let x1, x2 = x in
    let y1, y2 = y in
    equal_typ_val a c x1 y1 && equal_typ_val b d x2 y2
  | Table (a, b), Table (c, d) ->
    let x1, x2 = x in
    let y1, y2 = y in
    equal_typ_val a c x1 y1 && equal_typ_val b d x2 y2
  | Conv (p1, _, a), Conv (p2, _, b) ->
    equal_typ_val a b (p1 x) (p2 y)
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

let merge_tups ?(attr=NoAttr) a b = tups a b attr

let dict k v = Dict (k, v)
let table k v = Table (k, v)

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

let string_of_chars a =
  String.init (Array.length a) (Array.get a)

let guids a =
  let len = Bigstring.length a in
  if len mod 16 <> 0 then
    invalid_arg ("guids: " ^  string_of_int len) ;
  let nb_guids =  len / 16 in
  Array.init nb_guids begin fun i ->
    let guid = Bigstring.sub_string a (i*16) 16 in
    match Uuidm.of_bytes guid with
    | None -> assert false
    | Some v -> v
  end

let pp_print_ba pp ppf ba =
  let len = Array1.dim ba in
  for i = 0 to len - 1 do
    Format.fprintf ppf
      (if i = len - 1 then "%a" else "%a ") pp (Array1.get ba i)
  done

let pp_print_ba_uuid ppf ba =
  let len = Bigstring.length ba / 16 in
  for i = 0 to len - 1 do
    match (Uuidm.of_bytes (Bigstring.sub_string ba (i * 16) 16)) with
    | None -> invalid_arg "pp_print_ba_uuid"
    | Some guid ->
      Format.fprintf ppf
        (if i = len - 1 then "%a" else "%a ") Uuidm.pp guid
  done

let pp_print_month ppf i =
  let y, m, _ = month_of_int i in
  Format.fprintf ppf "%d.%dm" y m

let pp_print_date ppf i =
  let y, m, d = date_of_int i in
  Format.fprintf ppf "%d.%d.%d" y m d

let pp_print_timespan ppf j =
  let { time = ((hh, mm, ss), _) ; ns } = timespan_of_int64 j in
  Format.fprintf ppf "%d:%d:%d.%d" hh mm ss ns

let pp_print_minute ppf i =
  let (hh, mm, _), _ = minute_of_int i in
  Format.fprintf ppf "%d:%d" hh mm

let pp_print_second ppf i =
  let (hh, mm, ss), _ = second_of_int i in
  Format.fprintf ppf "%d:%d:%d" hh mm ss

let pp_print_time ppf i =
  let { time = ((hh, mm, ss), _) ; ms } = time_of_int i in
  Format.fprintf ppf "%d:%d:%d.%d" hh mm ss ms

let pp_print_symbols ppf syms =
  Array.iter (fun sym -> Format.fprintf ppf "`%s" sym) syms

let pp_print_timestamp ppf j =
  Format.fprintf ppf "%a" (Ptime.pp_rfc3339 ~frac_s:9 ()) (timestamp_of_int64 j)

let pp ppf (K (_, t, pp)) = Format.fprintf ppf "%a" pp t

let rec construct_list :
  type a. Buffer.t -> a w -> a -> unit = fun buf w a ->
  match w with
  | Tups (hw, tw, attr) ->
    let h, t = a in
    construct_list buf hw h ;
    construct_list buf tw t
  | Tup (w, attr) -> construct buf w a
  | _ -> assert false

and construct : type a. Buffer.t -> a w -> a -> unit = fun buf w a ->
  match w with
  | List (w', attr) ->
    Buffer.add_char buf '\x00' ;
    Buffer.add_char buf (char_of_attribute attr) ;
    Array.iter (construct buf w') a

  | Tup (ww, attr) -> construct buf ww a

  | Tups _ ->
    let ks = construct_list [] w a in
    let len = List.length ks in
    let k = ktn 0 len in
    List.iteri (kK_set k) ks ;
    K (w, k)

  | Dict (k, v) ->
    let x, y = a in
    let K (_, k') = construct k x in
    let K (_, k'') = construct v y in
    K (w, xD k' k'')

  | Table (k, v) ->
    let x, y = a in
    let K (_, k') = construct k x in
    let K (_, k'') = construct v y in
    begin match xT (xD k' k'') with
      | Error msg -> invalid_arg msg
      | Ok k ->  K (w, k)
    end
  | Conv (project, _, ww) ->
    let K (_, a) = construct ww (project a) in
    K (w, a)

  | Atom Boolean ->
    Buffer.add_char buf '\xff' ;
    begin match a with
      | false -> Buffer.add_char buf '\x00'
      | true -> Buffer.add_char buf '\x01'
    end

  | Atom Byte ->
    Buffer.add_char buf '\xfc' ;
    Buffer.add_char buf a

  | Atom Short ->
    let l = Bytes.create 2 in
    Buffer.add_char buf '\xfb' ;
    EndianString.NativeEndian.set_int16 l 0 a ;
    Buffer.add_bytes buf l

  | Atom Int ->
    let l = Bytes.create 4 in
    Buffer.add_char buf '\xfa' ;
    EndianString.NativeEndian.set_int32 l 0 a ;
    Buffer.add_bytes buf l

  | Atom Long ->
    let l = Bytes.create 8 in
    Buffer.add_char buf '\xf9' ;
    EndianString.NativeEndian.set_int64 l 0 a ;
    Buffer.add_bytes buf l

  | Atom Real ->
    let l = Bytes.create 4 in
    Buffer.add_char buf '\xf8' ;
    EndianString.NativeEndian.set_float l 0 a ;
    Buffer.add_bytes buf l

  | Atom Float ->
    let l = Bytes.create 8 in
    Buffer.add_char buf '\xf7' ;
    EndianString.NativeEndian.set_double l 0 a ;
    Buffer.add_bytes buf l

  | Atom Char ->
    Buffer.add_char buf '\xf6' ;
    Buffer.add_char buf a

  | Atom Symbol ->
    Buffer.add_char buf '\xf5' ;
    Buffer.add_string buf a ;
    Buffer.add_char buf '\x00'

  | Atom Guid ->
    Buffer.add_char buf '\xfe' ;
    Buffer.add_string buf (Uuidm.to_string a)

  | Atom Date ->
    Buffer.add_char buf '\xf2' ;
    let l = Bytes.create 4 in
    EndianString.NativeEndian.set_int32 l 0 (Int32.of_int (int_of_date a)) ;
    Buffer.add_bytes buf l

  | Atom Time ->
    Buffer.add_char buf '\xed' ;
    let l = Bytes.create 4 in
    EndianString.NativeEndian.set_int32 l 0 (Int32.of_int (int_of_time a)) ;
    Buffer.add_bytes buf l

  | Atom Timespan ->
    Buffer.add_char buf '\xf0' ;
    let l = Bytes.create 8 in
    EndianString.NativeEndian.set_int64 l 0 (int64_of_timespan a) ;
    Buffer.add_bytes buf l

  | Atom Timestamp ->
    Buffer.add_char buf '\xf4' ;
    let l = Bytes.create 8 in
    EndianString.NativeEndian.set_int64 l 0 (int64_of_timestamp a) ;
    Buffer.add_bytes buf l

  | Atom Month ->
    Buffer.add_char buf '\xf3' ;
    let l = Bytes.create 4 in
    EndianString.NativeEndian.set_int32 l 0 (Int32.of_int (int_of_month a)) ;
    Buffer.add_bytes buf l

  | Atom Minute ->
    Buffer.add_char buf '\xef' ;
    let l = Bytes.create 4 in
    EndianString.NativeEndian.set_int32 l 0 (Int32.of_int (int_of_minute a)) ;
    Buffer.add_bytes buf l

  | Atom Second ->
    Buffer.add_char buf '\xed' ;
    let l = Bytes.create 4 in
    EndianString.NativeEndian.set_int32 l 0 (Int32.of_int (int_of_second a)) ;
    Buffer.add_bytes buf l

  | Vect (Boolean, attr) ->
    Buffer.add_char buf '\x01' ;
    Buffer.add_char buf (char_of_attribute attr) ;
    let l = Bytes.create 4 in
    EndianString.NativeEndian.set_int32 l 0 (Int32.of_int (Array.length a)) ;
    Buffer.add_bytes buf l ;
    Array.iteri begin fun i -> function
      | false -> Buffer.add_char buf '\x00'
      | true -> Buffer.add_char buf '\x01'
    end a

  | String (Byte, attr) ->
    Buffer.add_char buf '\x04' ;
    Buffer.add_char buf (char_of_attribute attr) ;
    let l = Bytes.create 4 in
    EndianString.NativeEndian.set_int32 l 0 (Int32.of_int (String.length a)) ;
    Buffer.add_bytes buf l ;
    Buffer.add_string buf a

  | Vect (Byte, attr) ->
    Buffer.add_char buf '\x04' ;
    Buffer.add_char buf (char_of_attribute attr) ;
    let l = Bytes.create 4 in
    EndianString.NativeEndian.set_int32 l 0 (Int32.of_int (Array.length a)) ;
    Buffer.add_bytes buf l ;
    Array.iter (Buffer.add_char buf) a

  | String (Char, attr) ->
    Buffer.add_char buf '\x0a' ;
    Buffer.add_char buf (char_of_attribute attr) ;
    let l = Bytes.create 4 in
    EndianString.NativeEndian.set_int32 l 0 (Int32.of_int (String.length a)) ;
    Buffer.add_bytes buf l ;
    Buffer.add_string buf a

  | Vect (Char, attr) ->
    Buffer.add_char buf '\x0a' ;
    Buffer.add_char buf (char_of_attribute attr) ;
    let l = Bytes.create 4 in
    EndianString.NativeEndian.set_int32 l 0 (Int32.of_int (Array.length a)) ;
    Buffer.add_bytes buf l ;
    Array.iter (Buffer.add_char buf) a

  | Vect (Short, attr) ->
    Buffer.add_char buf '\x05' ;
    Buffer.add_char buf (char_of_attribute attr) ;
    let l = Bytes.create 4 in
    EndianString.NativeEndian.set_int32 l 0 (Int32.of_int (Array.length a)) ;
    Buffer.add_bytes buf l ;
    Array.iter begin fun i ->
      EndianString.NativeEndian.set_int16 l 0 i ;
      Buffer.add_subbytes buf l 0 2
    end a

  | Vect (Int, attr) ->
    Buffer.add_char buf '\x06' ;
    Buffer.add_char buf (char_of_attribute attr) ;
    let l = Bytes.create 4 in
    EndianString.NativeEndian.set_int32 l 0 (Int32.of_int (Array.length a)) ;
    Buffer.add_bytes buf l ;
    Array.iter begin fun i ->
      EndianString.NativeEndian.set_int32 l 0 i ;
      Buffer.add_bytes buf l
    end a

  | Vect (Long, attr) ->
    Buffer.add_char buf '\x07' ;
    Buffer.add_char buf (char_of_attribute attr) ;
    let l = Bytes.create 8 in
    EndianString.NativeEndian.set_int32 l 0 (Int32.of_int (Array.length a)) ;
    Buffer.add_subbytes buf l 0 4 ;
    Array.iter begin fun i ->
      EndianString.NativeEndian.set_int64 l 0 i ;
      Buffer.add_bytes buf l
    end a

  | Vect (Real, attr) ->
    Buffer.add_char buf '\x08' ;
    Buffer.add_char buf (char_of_attribute attr) ;
    let l = Bytes.create 4 in
    EndianString.NativeEndian.set_int32 l 0 (Int32.of_int (Array.length a)) ;
    Buffer.add_bytes buf l ;
    Array.iter begin fun i ->
      EndianString.NativeEndian.set_float l 0 i ;
      Buffer.add_bytes buf l
    end a

  | Vect (Float, attr) ->
    Buffer.add_char buf '\x09' ;
    Buffer.add_char buf (char_of_attribute attr) ;
    let l = Bytes.create 8 in
    EndianString.NativeEndian.set_int32 l 0 (Int32.of_int (Array.length a)) ;
    Buffer.add_subbytes buf l 0 4 ;
    Array.iter begin fun i ->
      EndianString.NativeEndian.set_double l 0 i ;
      Buffer.add_bytes buf l
    end a

  | Vect (Symbol, attr) ->
    Buffer.add_char buf '\x0b' ;
    Buffer.add_char buf (char_of_attribute attr) ;
    let l = Bytes.create 4 in
    EndianString.NativeEndian.set_int32 l 0 (Int32.of_int (Array.length a)) ;
    Buffer.add_bytes buf l ;
    Array.iter begin fun s ->
      Buffer.add_string buf s ;
      Buffer.add_char buf '\x00'
    end a

  | Vect (Guid, attr) ->
    Buffer.add_char buf '\x02' ;
    Buffer.add_char buf (char_of_attribute attr) ;
    let l = Bytes.create 4 in
    EndianString.NativeEndian.set_int32 l 0 (Int32.of_int (Array.length a)) ;
    Buffer.add_bytes buf l ;
    Array.iter begin fun s ->
      Buffer.add_string buf (Uuidm.to_bytes s) ;
    end a

  | Vect (Timestamp, attr) ->
    Buffer.add_char buf '\x0c' ;
    Buffer.add_char buf (char_of_attribute attr) ;
    let l = Bytes.create 8 in
    EndianString.NativeEndian.set_int32 l 0 (Int32.of_int (Array.length a)) ;
    Buffer.add_subbytes buf l 0 4 ;
    Array.iter begin fun ts ->
      EndianString.NativeEndian.set_int64 l 0 (int64_of_timestamp ts) ;
      Buffer.add_bytes buf l ;
    end a

  | Vect (Month, attr) ->
    Buffer.add_char buf '\x0d' ;
    Buffer.add_char buf (char_of_attribute attr) ;
    let l = Bytes.create 4 in
    EndianString.NativeEndian.set_int32 l 0 (Int32.of_int (Array.length a)) ;
    Buffer.add_bytes buf l ;
    Array.iter begin fun m ->
      EndianString.NativeEndian.set_int32 l 0 (Int32.of_int (int_of_month m)) ;
      Buffer.add_bytes buf l
    end a

  | Vect (Date, attr) ->
    Buffer.add_char buf '\x0e' ;
    Buffer.add_char buf (char_of_attribute attr) ;
    let l = Bytes.create 4 in
    EndianString.NativeEndian.set_int32 l 0 (Int32.of_int (Array.length a)) ;
    Buffer.add_bytes buf l ;
    Array.iter begin fun m ->
      EndianString.NativeEndian.set_int32 l 0 (Int32.of_int (int_of_date m)) ;
      Buffer.add_bytes buf l
    end a

  | Vect (Timespan, attr) ->
    Buffer.add_char buf '\x10' ;
    Buffer.add_char buf (char_of_attribute attr) ;
    let l = Bytes.create 8 in
    EndianString.NativeEndian.set_int32 l 0 (Int32.of_int (Array.length a)) ;
    Buffer.add_subbytes buf l 0 4 ;
    Array.iter begin fun a ->
      EndianString.NativeEndian.set_int64 l 0 (int64_of_timespan a) ;
      Buffer.add_bytes buf l ;
    end a

  | Vect (Minute, attr) ->
    Buffer.add_char buf '\x11' ;
    Buffer.add_char buf (char_of_attribute attr) ;
    let l = Bytes.create 4 in
    EndianString.NativeEndian.set_int32 l 0 (Int32.of_int (Array.length a)) ;
    Buffer.add_bytes buf l ;
    Array.iter begin fun m ->
      EndianString.NativeEndian.set_int32 l 0 (Int32.of_int (int_of_minute m)) ;
      Buffer.add_bytes buf l
    end a

  | Vect (Second, attr) ->
    Buffer.add_char buf '\x12' ;
    Buffer.add_char buf (char_of_attribute attr) ;
    let l = Bytes.create 4 in
    EndianString.NativeEndian.set_int32 l 0 (Int32.of_int (Array.length a)) ;
    Buffer.add_bytes buf l ;
    Array.iter begin fun m ->
      EndianString.NativeEndian.set_int32 l 0 (Int32.of_int (int_of_second m)) ;
      Buffer.add_bytes buf l
    end a

  | Vect (Time, attr) ->
    Buffer.add_char buf '\x13' ;
    Buffer.add_char buf (char_of_attribute attr) ;
    let l = Bytes.create 4 in
    EndianString.NativeEndian.set_int32 l 0 (Int32.of_int (Array.length a)) ;
    Buffer.add_bytes buf l ;
    Array.iter begin fun a ->
      EndianString.NativeEndian.set_int32 l 0 (Int32.of_int (int_of_time a)) ;
      Buffer.add_bytes buf l
    end a

  | String _ -> assert false

let rec destruct_list : type a. a w -> k -> int -> (a * int, string) result = fun w k i ->
  match w with
  | Tup a -> begin
      match destruct a (kK k i) with
      | Error e -> Error e
      | Ok a -> Ok (a, succ i)
    end
  | Tups (h, t) -> begin
      match destruct_list h k i with
      | Error msg -> Error msg
      | Ok (v, i) -> match destruct_list t k i with
        | Error e -> Error e
        | Ok (vv, i) -> Ok ((v, vv), i)
    end
  | _ -> assert false

and destruct : type a. a w -> k -> (a, string) result = fun w k ->
  match w with
  | List w when k_objtyp k = 0 -> begin
      let rec inner acc = function
        | -1 -> acc
        | i ->
          match destruct w (kK k i) with
          | Error e -> failwith e
          | Ok v -> inner (v :: acc) (pred i) in
      try
        Ok (Array.of_list (inner [] (pred (k_length k))))
      with Failure msg -> Error msg
    end
  | Conv (_, inject, w) -> begin
      match destruct w k with
      | Error e -> Error e
      | Ok v -> Ok (inject v)
    end
  | Tup w when k_objtyp k = 0 -> destruct w (kK k 0)
  | Tups _ when k_objtyp k = 0 ->
    (match destruct_list w k 0 with Error e -> Error e | Ok (v, _) -> Ok v)
  | Table (key, values) when k_objtyp k = 98 ->
    destruct (Dict (key, values)) (k_k k)
  | Dict (kw, vw) when k_objtyp k = 99 -> begin
      match destruct kw (kK k 0), destruct vw (kK k 1) with
      | Ok a, Ok b -> Ok (a, b)
      | Error e, _
      | _, Error e -> Error e
    end

  | Atom Boolean when k_objtyp k = -1 -> Ok (k_g k <> 0)
  | Atom Guid when k_objtyp k = -2 -> Ok (k_u k)
  | Atom Byte when k_objtyp k = -4 -> Ok (Char.chr (k_g k))
  | Atom Short when k_objtyp k = -5 -> Ok (k_h k)
  | Atom Int when k_objtyp k = -6 -> Ok (k_i k)
  | Atom Long when k_objtyp k = -7 -> Ok (k_j k)
  | Atom Real when k_objtyp k = -8 -> Ok (k_e k)
  | Atom Float when k_objtyp k = -9 -> Ok (k_f k)
  | Atom Char when k_objtyp k = -10 -> Ok (Char.chr (k_g k))
  | Atom Symbol when k_objtyp k = -11 -> Ok (k_s k)
  | Atom Timestamp when k_objtyp k = -12 -> Ok (timestamp_of_int64 (k_j k))
  | Atom Month when k_objtyp k = -13 -> Ok (month_of_int (k_ii k))
  | Atom Date when k_objtyp k = -14 -> Ok (date_of_int (k_ii k))
  | Atom Timespan when k_objtyp k = -16 -> Ok (timespan_of_int64 (k_j k))
  | Atom Minute when k_objtyp k = -17 -> Ok (minute_of_int (k_ii k))
  | Atom Second when k_objtyp k = -18 -> Ok (second_of_int (k_ii k))
  | Atom Time when k_objtyp k = -19 -> Ok (time_of_int (k_ii k))

  | Vect Boolean when k_objtyp k = 1 ->
    let len = k_length k in
    let buf = kG_char k in
    Ok (Array.init len (fun i -> Bigstring.get buf i <> '\x00'))

  | Vect Guid when k_objtyp k = 2 -> Ok (guids (kU k))

  | Vect Byte when k_objtyp k = 4 ->
    let len = k_length k in
    let buf = kG_char k in
    Ok (Array.init len (Bigstring.get buf))

  | String Byte when k_objtyp k = 4 ->
    let buf = kG_char k in
    Ok (Bigstring.to_string buf)

  | Vect Short when k_objtyp k = 5 ->
    let len = k_length k in
    let buf = kH k in
    Ok (Array.init len (Array1.get buf))

  | Vect Int when k_objtyp k = 6 ->
    let len = k_length k in
    let buf = kI k in
    Ok (Array.init len (Array1.get buf))

  | Vect Long when k_objtyp k = 7 ->
    let len = k_length k in
    let buf = kJ k in
    Ok (Array.init len (Array1.get buf))

  | Vect Real when k_objtyp k = 8 ->
    let len = k_length k in
    let buf = kE k in
    Ok (Array.init len (Array1.get buf))

  | Vect Float when k_objtyp k = 9 ->
    let len = k_length k in
    let buf = kF k in
    Ok (Array.init len (Array1.get buf))

  | Vect Char when k_objtyp k = 10 ->
    let len = k_length k in
    let buf = kG_char k in
    Ok (Array.init len (Bigstring.get buf))

  | String Char when k_objtyp k = 10 ->
    let buf = kG_char k in
    Ok (Bigstring.to_string buf)

  | Vect Symbol when k_objtyp k = 11 ->
    let len = k_length k in
    Ok (Array.init len (kS k))

  | Vect Timestamp when k_objtyp k = 12 ->
    let len = k_length k in
    let buf = kJ k in
    Ok (Array.init len (fun i -> timestamp_of_int64 (Array1.get buf i)))

  | Vect Month when k_objtyp k = 13 ->
    let len = k_length k in
    let buf = kI k in
    Ok (Array.init len (fun i -> month_of_int (Int32.to_int @@ Array1.get buf i)))

  | Vect Date when k_objtyp k = 14 ->
    let len = k_length k in
    let buf = kI k in
    Ok (Array.init len (fun i -> date_of_int (Int32.to_int @@ Array1.get buf i)))

  | Vect Timespan when k_objtyp k = 16 ->
    let len = k_length k in
    let buf = kJ k in
    Ok (Array.init len (fun i -> timespan_of_int64 (Array1.get buf i)))

  | Vect Minute when k_objtyp k = 17 ->
    let len = k_length k in
    let buf = kI k in
    Ok (Array.init len (fun i -> minute_of_int (Int32.to_int @@ Array1.get buf i)))

  | Vect Second when k_objtyp k = 18 ->
    let len = k_length k in
    let buf = kI k in
    Ok (Array.init len (fun i -> second_of_int (Int32.to_int @@ Array1.get buf i)))

  | Vect Time when k_objtyp k = 19 ->
    let len = k_length k in
    let buf = kI k in
    Ok (Array.init len (fun i -> time_of_int (Int32.to_int @@ Array1.get buf i)))

  | List _ ->
    Error (Printf.sprintf "got type %d, expected %d" (k_objtyp k) (int_of_w w))
  | Atom _ ->
    Error (Printf.sprintf "got type %d, expected %d" (k_objtyp k) (int_of_w w))
  | Vect _ ->
    Error (Printf.sprintf "got type %d, expected %d" (k_objtyp k) (int_of_w w))
  | String _ ->
    Error (Printf.sprintf "got type %d, expected %d" (k_objtyp k) (int_of_w w))
  | Tup _ ->
    Error (Printf.sprintf "got type %d, expected %d" (k_objtyp k) (int_of_w w))
  | Tups _ ->
    Error (Printf.sprintf "got type %d, expected %d" (k_objtyp k) (int_of_w w))
  | Dict _ ->
    Error (Printf.sprintf "got type %d, expected %d" (k_objtyp k) (int_of_w w))
  | Table _ ->
    Error (Printf.sprintf "got type %d, expected %d" (k_objtyp k) (int_of_w w))

(* Communication with kdb+ *)

external khp : string -> int -> int = "khp_stub"
external khpu : string -> int -> string -> int = "khpu_stub"
external khpun : string -> int -> string -> int -> int = "khpun_stub"
external khpunc : string -> int -> string -> int -> int -> int = "khpunc_stub"
external kclose : Unix.file_descr -> unit = "kclose_stub" [@@noalloc]

external kread : Unix.file_descr -> k = "kread_stub"
external k0 : Unix.file_descr -> string -> unit = "k0_stub" [@@noalloc]
external k1 : Unix.file_descr -> string -> k -> unit = "k1_stub" [@@noalloc]
external k2 : Unix.file_descr -> string -> k -> k -> unit = "k2_stub" [@@noalloc]
external k3 : Unix.file_descr -> string -> k -> k -> k -> unit = "k3_stub" [@@noalloc]
external kn : Unix.file_descr -> string -> k array -> unit = "kn_stub" [@@noalloc]

external k0_sync : Unix.file_descr -> string -> k = "k0_sync_stub"
external k1_sync : Unix.file_descr -> string -> k -> k = "k1_sync_stub"
external k2_sync : Unix.file_descr -> string -> k -> k -> k = "k2_sync_stub"
external k3_sync : Unix.file_descr -> string -> k -> k -> k -> k = "k3_sync_stub"
external kn_sync : Unix.file_descr -> string -> k array -> k = "kn_sync_stub"

let kread fd w = destruct w (kread fd)

let k1 fd f (K (_, a)) = k1 fd f a
let k2 fd f (K (_, a)) (K (_, b)) = k2 fd f a b
let k3 fd f (K (_, a)) (K (_, b)) (K (_, c)) = k3 fd f a b c
let kn fd f a = kn fd f (Array.map (function K (_, k) -> k) a)

let k0_sync fd f w = destruct w (k0_sync fd f)
let k1_sync fd f w (K (_, a)) = destruct w (k1_sync fd f a)
let k2_sync fd f w (K (_, a)) (K (_, b)) = destruct w (k2_sync fd f a b)
let k3_sync fd f w (K (_, a)) (K (_, b)) (K (_, c)) = destruct w (k3_sync fd f a b c)
let kn_sync fd f w a = destruct w (kn_sync fd f (Array.map (function K (_, k) -> k) a))

let destruct_k = destruct
let destruct w (K (_, k)) = destruct_k w k

let equal_typ (K (w1, _)) (K (w2, _)) = equal w1 w2
let equal (K (w1, a)) (K (w2, b)) =
  match destruct_k w1 a, destruct_k w2 b with
  | Ok aa, Ok bb -> equal_typ_val w1 w2 aa bb
  | Error e, Ok _ -> failwith ("first failed: " ^ e)
  | Ok _, Error e -> failwith ("second failed: " ^ e)
  | Error e, Error f -> failwith ("both failed: " ^ e ^ ", " ^ f)

(* Serialization *)

external b9 : int -> k -> (k, string) result = "b9_stub"
external d9 : k -> (k, string) result = "d9_stub"

let of_string :
  type a. a w -> string -> (t, string) result = fun w bs ->
  let K (_, k) = construct (s byte) bs in
  match d9 k with
  | Error e -> Error e
  | Ok a -> Ok (K (w, a))

let of_string_exn w s =
  match (of_string w s) with
  | Error msg -> invalid_arg msg
  | Ok s -> s

let to_string ?(mode = -1) (K (_, k)) =
  match b9 mode k with
  | Error e -> failwith e
  | Ok k ->
    match destruct_k (s byte) k with
    | Error msg -> invalid_arg msg
    | Ok s -> s

type connection_error =
  | Authentication
  | Connection
  | Timeout
  | OpenSSL

let pp_connection_error ppf = function
  | Authentication -> Format.pp_print_string ppf "authentification error"
  | Connection -> Format.pp_print_string ppf "connection error"
  | Timeout -> Format.pp_print_string ppf "timeout error"
  | OpenSSL -> Format.pp_print_string ppf "tls error"

type capability =
  | OneTBLimit
  | UseTLS

let int_of_capability = function
  | OneTBLimit -> 1
  | UseTLS -> 2

let wrap_result f =
  match f () with
  | i when i > 0 -> Ok (Obj.magic i : Unix.file_descr)
  | 0 -> Error Authentication
  | -1 -> Error Connection
  | -2 -> Error Timeout
  | -3 -> Error OpenSSL
  | i -> failwith ("Unknown q error " ^ string_of_int i)

let up u p = u ^ ":" ^ p

let init () = ignore (khp "" ~-1)

let connect ?timeout ?capability url =
  let host = Uri.host_with_default ~default:"localhost" url in
  let userinfo =
    match Uri.user url, Uri.password url with
    | Some u, Some p -> Some (u, p)
    | _ -> None in
  match Uri.port url with
  | None -> invalid_arg "connect: port unspecified"
  | Some port ->
    match userinfo, timeout, capability with
    | None, None, None -> wrap_result (fun () -> khp host port)
    | Some (u, p), None, None -> wrap_result (fun () -> khpu host port (up u p))
    | Some (u, p), Some t, None ->
      let t = int_of_float (Ptime.Span.to_float_s t /. 1e3) in
      wrap_result (fun () -> khpun host port (up u p) t)
    | Some (u, p), Some t, Some c ->
      let t = int_of_float (Ptime.Span.to_float_s t /. 1e3) in
      wrap_result (fun () -> khpunc host port (up u p) t (int_of_capability c))
    | _ -> invalid_arg "connect"

let with_connection ?timeout ?capability url ~f =
  match connect ?timeout ?capability url with
  | Error e -> Error e
  | Ok fd ->
    let ret = f fd in
    kclose fd ;
    Ok ret

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
