(*---------------------------------------------------------------------------
   Copyright (c) 2018 Vincent Bernardoff. All rights reserved.
   Distributed under the ISC license, see terms at the end of the file.
  ---------------------------------------------------------------------------*)

type k

type ('a, 'b) storage = ('a, 'b, Bigarray.c_layout) Bigarray.Array1.t

type bool_arr    = (bool, Bigarray.int8_unsigned_elt) storage
type char_arr    = (char, Bigarray.int8_unsigned_elt) storage
type uint8_arr   = (int, Bigarray.int8_unsigned_elt) storage
type int16_arr   = (int, Bigarray.int16_signed_elt) storage
type int32_arr   = (int32, Bigarray.int32_elt) storage
type int64_arr   = (int32, Bigarray.int32_elt) storage
type float32_arr = (float, Bigarray.float32_elt) storage
type float64_arr = (float, Bigarray.float64_elt) storage

(* let to_array a =
 *   let len = Bigarray.Array1.dim a in
 *   Array.init len (Bigarray.Array1.get a) *)

type _ kw =
  | Bool      : bool_arr kw
  | Guid      : uint8_arr kw
  | Byte      : uint8_arr kw
  | Short     : int16_arr kw
  | Int       : int32_arr kw
  | Long      : int64_arr kw
  | Real      : float32_arr kw
  | Float     : float64_arr kw
  | Char      : char_arr kw
  | Symbol    : string array kw
  | Timestamp : int64_arr kw
  | Month     : int32_arr kw
  | Date      : int32_arr kw
  | Timespan  : int64_arr kw
  | Minute    : int32_arr kw
  | Second    : int32_arr kw
  | Time      : int32_arr kw
  | Datetime  : float64_arr kw

let int_of_kw : type a. a kw -> int = function
  | Bool      -> 1
  | Guid      -> 2
  | Byte      -> 4
  | Short     -> 5
  | Int       -> 6
  | Long      -> 7
  | Real      -> 8
  | Float     -> 9
  | Char      -> 10
  | Symbol    -> 11
  | Timestamp -> 12
  | Month     -> 13
  | Date      -> 14
  | Timespan  -> 16
  | Minute    -> 17
  | Second    -> 18
  | Time      -> 19
  | Datetime  -> 15

let bool      = Bool
let guid      = Guid
let byte      = Byte
let short     = Short
let int       = Int
let long      = Long
let real      = Real
let float     = Float
let char      = Char
let symbol    = Symbol
let timestamp = Timestamp
let month     = Month
let date      = Date
let timespan  = Timespan
let minute    = Minute
let second    = Second
let time      = Time
let datetime  = Datetime

type (_,_) eq = Eq : ('a,'a) eq

let eq_kw : type a b. a kw -> b kw -> (a,b) eq option = fun a b ->
  match a, b with
  | Bool, Bool             -> Some Eq
  | Guid, Guid             -> Some Eq
  | Byte, Byte             -> Some Eq
  | Short, Short           -> Some Eq
  | Int, Int               -> Some Eq
  | Long, Long             -> Some Eq
  | Real, Real             -> Some Eq
  | Float, Float           -> Some Eq
  | Char, Char             -> Some Eq
  | Symbol, Symbol         -> Some Eq
  | Timestamp, Timestamp   -> Some Eq
  | Month, Month           -> Some Eq
  | Date, Date             -> Some Eq
  | Timespan, Timespan     -> Some Eq
  | Minute, Minute         -> Some Eq
  | Second, Second         -> Some Eq
  | Time, Time             -> Some Eq
  | Datetime, Datetime     -> Some Eq
  | _                      -> None

type vector = Vect : 'a kw * 'a -> vector

let bool_vect v      = Vect (bool, v)
let guid_vect v      = Vect (guid, v)
let byte_vect v      = Vect (byte, v)
let short_vect v     = Vect (short, v)
let int_vect v       = Vect (int, v)
let long_vect v      = Vect (long, v)
let real_vect v      = Vect (real, v)
let float_vect v     = Vect (float, v)
let char_vect v      = Vect (char, v)
let symbol_vect v    = Vect (symbol, v)
let timestamp_vect v = Vect (timestamp, v)
let month_vect v     = Vect (month, v)
let date_vect v      = Vect (date, v)
let timespan_vect v  = Vect (timespan, v)
let minute_vect v    = Vect (minute, v)
let second_vect v    = Vect (second, v)
let time_vect v      = Vect (time, v)
let datetime_vect v  = Vect (datetime, v)

let get_vector :
  type a. a kw -> vector -> a option = fun a (Vect(b,x)) ->
  match eq_kw a b with
  | None -> None
  | Some Eq -> Some x

external r0 : k -> unit = "r0_stub" [@@noalloc]

(* Accessors *)

external k_objtyp : k -> int = "k_objtyp" [@@noalloc]
external k_objattrs : k -> int = "k_objattrs" [@@noalloc]
external k_refcount : k -> int = "k_refcount" [@@noalloc]
external k_length : k -> int64 = "k_length"

external k_g : k -> int = "k_g" [@@noalloc]
external k_h : k -> int = "k_h" [@@noalloc]

external k_i : k -> int32 = "k_i"
external k_j : k -> int64 = "k_j"
external k_e : k -> float = "k_e"
external k_f : k -> float = "k_f"

external k_s : k -> string = "k_s"
external k_k : k -> k = "k_k"
external k_u : k -> string = "k_u"
let k_u k =
  match Uuidm.of_bytes (k_u k) with
  | None -> invalid_arg "k_u"
  | Some u -> u

(* List accessors *)

external kK : k -> int -> k = "kK_stub"
external kK_set : k -> int -> k -> unit = "kK_set_stub"
external kS : k -> int -> string = "kS_stub"
external kS_set : k -> int -> string -> unit = "kS_set_stub"

external kG_bool : k -> bool_arr = "kG_stub"
let kG_bool k =
  let r = kG_bool k in
  Gc.finalise_last (fun () -> r0 k) r ;
  r
external kG_char : k -> char_arr = "kG_stub"
let kG_char k =
  let r = kG_char k in
  Gc.finalise_last (fun () -> r0 k) r ;
  r
external kG : k -> uint8_arr = "kG_stub"
let kG k =
  let r = kG k in
  Gc.finalise_last (fun () -> r0 k) r ;
  r
external kH : k -> int16_arr = "kH_stub"
let kH k =
  let r = kH k in
  Gc.finalise_last (fun () -> r0 k) r ;
  r
external kI : k -> int32_arr = "kI_stub"
let kI k =
  let r = kI k in
  Gc.finalise_last (fun () -> r0 k) r ;
  r
external kJ : k -> int64_arr = "kJ_stub"
let kJ k =
  let r = kJ k in
  Gc.finalise_last (fun () -> r0 k) r ;
  r
external kE : k -> float32_arr = "kE_stub"
let kE k =
  let r = kE k in
  Gc.finalise_last (fun () -> r0 k) r ;
  r
external kF : k -> float64_arr = "kF_stub"
let kF k =
  let r = kF k in
  Gc.finalise_last (fun () -> r0 k) r ;
  r

(* Atom constructors *)

external kb : bool -> k = "kb_stub"
external ku : string -> k = "ku_stub"

external kc : char -> k = "kc_stub"
external kg : int -> k = "kg_stub"
external kh : int -> k = "kh_stub"
external ki : int32 -> k = "ki_stub"
external kj : int64 -> k = "kj_stub"

external ke : float -> k = "ke_stub"
external kf : float -> k = "kf_stub"

external ks : string -> k = "ks_stub"

external kt : int32 -> k = "kt_stub"
external kd : int32 -> k = "kd_stub"
external kmonth : int32 -> k = "kmonth_stub"
external kminute : int32 -> k = "kminute_stub"
external ksecond : int32 -> k = "ksecond_stub"

external ktimestamp : int64 -> k = "ktimestamp_stub"
external ktimespan : int64 -> k = "ktimespan_stub"
external kz : float -> k = "kz_stub"

(* List constructors *)

external ktn : int -> int64 -> k = "ktn_stub"

(* Dict/Table accessors *)

external xD : k -> k -> k = "xD_stub"
external xT : k -> k = "xT_stub"
(* external ktd : k -> k = "ktd_stub" *)

(* external ja_int : k -> int -> unit = "ja_int_stub" *)
(* external ja_long : k -> int -> unit = "ja_long_stub" *)
(* external ja_int32 : k -> int32 -> unit = "ja_int32_stub"
 * external ja_int64 : k -> int64 -> unit = "ja_int32_stub"
 * external ja_double : k -> float -> unit = "ja_double_stub"
 * external ja_bool : k -> bool -> unit = "ja_double_stub"
 * external ja_uuid : k -> string -> unit = "ja_uuid_stub"
 * external js : k -> string -> unit = "js_stub" *)

let kx_epoch, kx_epoch_span =
  match Ptime.of_date (2000, 1, 1) with
  | None -> assert false
  | Some t -> t, Ptime.to_span t

type t =
  | Atom      of atom
  | Vector    of vector
  | List      of t array
  | Dict      of t * t
  | Table     of t * t
and atom =
  | Bool      of bool
  | Guid      of Uuidm.t
  | Byte      of int
  | Short     of int
  | Int       of int32
  | Long      of int64
  | Real      of float
  | Float     of float
  | Char      of char
  | Symbol    of string
  | Timestamp of Ptime.t
  | Month     of int
  | Date      of int
  | Timespan  of Ptime.time * int
  | Minute    of int * int
  | Second    of int * int * int
  | Time      of Ptime.time * int
  | Datetime  of float

let day_in_ns d =
  Int64.(mul (of_int (d * 24 * 3600)) 1_000_000_000L)

let rec pack_atom = function
  | Bool b       -> kb b
  | Guid u       -> ku (Uuidm.to_bytes u)
  | Byte i       -> kg i
  | Short i      -> kh i
  | Int i        -> ki i
  | Long j       -> kj j
  | Real f       -> ke f
  | Float f      -> kf f
  | Char c       -> kc c
  | Symbol s     -> ks s
  | Timestamp ts ->
      let span_since_kxepoch =
        Ptime.(Span.sub (to_span ts) kx_epoch_span) in
      let d, ps = Ptime.Span.to_d_ps span_since_kxepoch in
      let ts = Int64.(add (day_in_ns d) (div ps 1_000L)) in
      ktimestamp ts
  | Time (((h, m, s), tz_offset), ms) ->
    let open Int32 in
    let ts =
      add
        (mul (of_int ((h - tz_offset) * 3600 + m * 60 + s)) 1_000l)
        (of_int ms) in
    kt ts
  | Timespan (((h, m, s), tz_offset), ns)  -> begin
      let open Int64 in
      let ts = add
          (mul (of_int ((h - tz_offset) * 3600 + m * 60 + s)) 1_000_000_000L)
          (of_int ns) in
      ktimespan ts
    end
  | Month i -> kmonth (Int32.of_int i)
  | Date i -> kd (Int32.of_int i)
  | Minute (hh, mm) -> kminute (Int32.of_int (hh * 60 + mm))
  | Second (hh, mm, ss) -> ksecond (Int32.of_int (hh * 3600 + mm * 60 + ss))
  | Datetime f   -> kz f

and pack = function
  | Atom a -> pack_atom a
  | Vector v when get_vector bool v <> None -> begin
    match get_vector bool v with
    | None -> assert false
    | Some v ->
      let len = Int64.of_int (Bigarray.Array1.dim v) in
      let k = ktn (int_of_kw bool) len in
      let arr = kG_bool k in
      Bigarray.Array1.blit v arr ;
      k
  end
  | Vector v when get_vector guid v <> None -> begin
    match get_vector guid v with
    | None -> assert false
    | Some v ->
      let len = Int64.of_int (Bigarray.Array1.dim v) in
      let k = ktn (int_of_kw guid) len in
      let arr = kG k in
      Bigarray.Array1.blit v arr ;
      k
  end
  | Vector v when get_vector byte v <> None -> begin
    match get_vector byte v with
    | None -> assert false
    | Some v ->
      let len = Int64.of_int (Bigarray.Array1.dim v) in
      let k = ktn (int_of_kw byte) len in
      let arr = kG k in
      Bigarray.Array1.blit v arr ;
      k
  end
  | Vector v when get_vector short v <> None -> begin
    match get_vector short v with
    | None -> assert false
    | Some v ->
      let len = Int64.of_int (Bigarray.Array1.dim v) in
      let k = ktn (int_of_kw short) len in
      let arr = kH k in
      Bigarray.Array1.blit v arr ;
      k
  end
  | Vector v when get_vector int v <> None -> begin
    match get_vector int v with
    | None -> assert false
    | Some v ->
      let len = Int64.of_int (Bigarray.Array1.dim v) in
      let k = ktn (int_of_kw int) len in
      let arr = kI k in
      Bigarray.Array1.blit v arr ;
      k
  end
  | Vector v when get_vector long v <> None -> begin
    match get_vector long v with
    | None -> assert false
    | Some v ->
      let len = Int64.of_int (Bigarray.Array1.dim v) in
      let k = ktn (int_of_kw long) len in
      let arr = kJ k in
      Bigarray.Array1.blit v arr ;
      k
  end
  | Vector v when get_vector real v <> None -> begin
    match get_vector real v with
    | None -> assert false
    | Some v ->
      let len = Int64.of_int (Bigarray.Array1.dim v) in
      let k = ktn (int_of_kw real) len in
      let arr = kE k in
      Bigarray.Array1.blit v arr ;
      k
  end
  | Vector v when get_vector float v <> None -> begin
    match get_vector float v with
    | None -> assert false
    | Some v ->
      let len = Int64.of_int (Bigarray.Array1.dim v) in
      let k = ktn (int_of_kw float) len in
      let arr = kF k in
      Bigarray.Array1.blit v arr ;
      k
  end
  | Vector v when get_vector char v <> None -> begin
    match get_vector char v with
    | None -> assert false
    | Some v ->
      let len = Int64.of_int (Bigarray.Array1.dim v) in
      let k = ktn (int_of_kw char) len in
      let arr = kG_char k in
      Bigarray.Array1.blit v arr ;
      k
  end
  | Vector v when get_vector symbol v <> None -> begin
    match get_vector symbol v with
    | None -> assert false
    | Some v ->
      let len = Int64.of_int (Array.length v) in
      let k = ktn (int_of_kw symbol) len in
      Array.iteri (kS_set k) v ;
      k
  end
  | Vector v when get_vector timestamp v <> None -> begin
    match get_vector timestamp v with
    | None -> assert false
    | Some v ->
      let len = Int64.of_int (Bigarray.Array1.dim v) in
      let k = ktn (int_of_kw timestamp) len in
      let arr = kJ k in
      Bigarray.Array1.blit v arr ;
      k
  end
  | Vector v when get_vector month v <> None -> begin
    match get_vector month v with
    | None -> assert false
    | Some v ->
      let len = Int64.of_int (Bigarray.Array1.dim v) in
      let k = ktn (int_of_kw month) len in
      let arr = kI k in
      Bigarray.Array1.blit v arr ;
      k
  end
  | Vector v when get_vector date v <> None -> begin
    match get_vector date v with
    | None -> assert false
    | Some v ->
      let len = Int64.of_int (Bigarray.Array1.dim v) in
      let k = ktn (int_of_kw date) len in
      let arr = kI k in
      Bigarray.Array1.blit v arr ;
      k
  end
  | Vector v when get_vector timespan v <> None -> begin
    match get_vector timespan v with
    | None -> assert false
    | Some v ->
      let len = Int64.of_int (Bigarray.Array1.dim v) in
      let k = ktn (int_of_kw timespan) len in
      let arr = kJ k in
      Bigarray.Array1.blit v arr ;
      k
  end
  | Vector v when get_vector minute v <> None -> begin
    match get_vector minute v with
    | None -> assert false
    | Some v ->
      let len = Int64.of_int (Bigarray.Array1.dim v) in
      let k = ktn (int_of_kw minute) len in
      let arr = kI k in
      Bigarray.Array1.blit v arr ;
      k
  end
  | Vector v when get_vector second v <> None -> begin
    match get_vector second v with
    | None -> assert false
    | Some v ->
      let len = Int64.of_int (Bigarray.Array1.dim v) in
      let k = ktn (int_of_kw second) len in
      let arr = kI k in
      Bigarray.Array1.blit v arr ;
      k
  end
  | Vector v when get_vector time v <> None -> begin
    match get_vector time v with
    | None -> assert false
    | Some v ->
      let len = Int64.of_int (Bigarray.Array1.dim v) in
      let k = ktn (int_of_kw time) len in
      let arr = kI k in
      Bigarray.Array1.blit v arr ;
      k
  end
  | Vector v -> begin
    match get_vector datetime v with
    | None -> assert false
    | Some v ->
      let len = Int64.of_int (Bigarray.Array1.dim v) in
      let k = ktn (int_of_kw datetime) len in
      let arr = kF k in
      Bigarray.Array1.blit v arr ;
      k
  end
  | List ts ->
    let len = Int64.of_int (Array.length ts) in
    let k = ktn 0 len in
    Array.iteri (fun i t -> kK_set k i (pack t)) ts ;
    k
  | Dict (k, v) ->
    xD (pack k) (pack v)
  | Table (k, v) ->
    xT (xD (pack k) (pack v))

let rec unpack_atom k =
  match ~- (k_objtyp k) with
  | 1  -> (match k_g k with 0 -> Bool false | _ -> Bool true)
  | 2  -> Guid (k_u k)
  | 4  -> Byte (k_g k)
  | 5  -> Short (k_h k)
  | 6  -> Int (k_i k)
  | 7  -> Long (k_j k)
  | 8  -> Real (k_e k)
  | 9  -> Float (k_f k)
  | 10 -> Char (Char.chr (k_g k))
  | 11 -> Symbol (k_s k)
  | 12 ->
    let nanos_since_kxepoch = k_j k in
    let one_day_in_ns = day_in_ns 1 in
    let days_since_kxepoch =
      Int64.(to_int (div nanos_since_kxepoch one_day_in_ns)) in
    let remaining_ps =
      Int64.(mul (rem nanos_since_kxepoch one_day_in_ns) 1_000L) in
    let span =
      Ptime.Span.v (days_since_kxepoch, remaining_ps) in
    let ts =
      match Ptime.add_span kx_epoch span with
      | None -> assert false
      | Some ts -> ts in
    Timestamp ts
  | 13 -> Month (Int32.to_int (k_i k))
  | 14 -> Date (Int32.to_int (k_i k))
  | 16 ->
    let time = k_j k in
    let ns = Int64.(to_int (rem time 1_000_000_000L)) in
    let s = Int64.(to_int (div time 1_000_000_000L)) in
    let hh = s / 3600 in
    let mm = (s / 60) mod 60 in
    let ss = s mod 60 in
    Timespan (((hh, mm, ss), 0), ns)
  | 17 ->
    let nb_minutes = Int32.to_int (k_i k) in
    Minute (nb_minutes / 60, nb_minutes mod 60)
  | 18 ->
    let nb_seconds = Int32.to_int (k_i k) in
    let hh = nb_seconds / 3600 in
    let mm = (nb_seconds / 60) mod 60 in
    let ss = nb_seconds mod 60 in
    Second (hh, mm, ss)
  | 19 ->
    let time = Int32.to_int (k_i k) in
    let ms = time mod 1_000 in
    let s = time / 1_000 in
    let hh = s / 3600 in
    let mm = (s / 60) mod 60 in
    let ss = s mod 60 in
    Time (((hh, mm, ss), 0), ms)
  | 15 -> Datetime (k_f k)
  | _  -> invalid_arg "unpack_atom: not an atom"

and unpack_vector k =
  match k_objtyp k with
  | 1  -> bool_vect (kG_bool k)
  | 2  -> guid_vect (kG k)
  | 4  -> byte_vect (kG k)
  | 5  -> short_vect (kH k)
  | 6  -> int_vect (kI k)
  | 7  -> long_vect (kJ k)
  | 8  -> real_vect (kE k)
  | 9  -> float_vect (kF k)
  | 10 -> char_vect (kG_char k)
  | 11 ->
    let len = Int64.to_int (k_length k) in
    symbol_vect (Array.init len (kS k))
  | 12 -> timestamp_vect (kJ k)
  | 13 -> month_vect (kI k)
  | 14 -> date_vect (kI k)
  | 16 -> timespan_vect (kJ k)
  | 17 -> minute_vect (kI k)
  | 18 -> second_vect (kI k)
  | 19 -> time_vect (kI k)
  | 15 -> datetime_vect (kF k)
  | _  -> invalid_arg "unpack_vector: not a vector"

and unpack_list k =
  let len = Int64.to_int (k_length k) in
  Array.init len begin fun i ->
    let kk = kK k i in
    unpack kk
  end

and unpack_dict k =
  Dict (unpack (kK k 0), unpack (kK k 1))

and unpack_table k =
  let kk = k_k k in
  Table (unpack (kK kk 0), unpack (kK k 1))

and unpack k =
  match k_objtyp k with
  | 0            -> List (unpack_list k)
  | n when n < 0 -> Atom (unpack_atom k)
  | 99           -> unpack_dict k
  | 98           -> unpack_table k
  | _            -> Vector (unpack_vector k)

(* Serialization *)

external b9 : int -> k -> (k, string) result = "b9_stub"
external d9 : k -> (k, string) result = "d9_stub"

let of_bigstring s =
  d9 (pack (Vector (char_vect s)))

let of_bigstring_exn s =
  match (of_bigstring s) with
  | Error msg -> invalid_arg msg
  | Ok s -> s

let bigstring_of_k k =
  match k_objtyp k with
  | 10 -> Some (kG_char k)
  | _ -> None

let serialize ?(mode = ~-1) k =
  match b9 mode k with
  | Error e -> Error e
  | Ok r ->
    match get_vector char (unpack_vector r) with
    | None -> invalid_arg "serialize: internal error"
    | Some bs -> Ok bs

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
