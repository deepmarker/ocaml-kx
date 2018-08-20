(*---------------------------------------------------------------------------
   Copyright (c) 2018 Vincent Bernardoff. All rights reserved.
   Distributed under the ISC license, see terms at the end of the file.
  ---------------------------------------------------------------------------*)

type k

type bool_ba =
  (bool, Bigarray.int8_unsigned_elt, Bigarray.c_layout) Bigarray.Array1.t
type char_ba =
  (char, Bigarray.int8_unsigned_elt, Bigarray.c_layout) Bigarray.Array1.t
type uint8_ba =
  (int, Bigarray.int8_unsigned_elt, Bigarray.c_layout) Bigarray.Array1.t
type int16_ba =
  (int, Bigarray.int16_signed_elt, Bigarray.c_layout) Bigarray.Array1.t
type int32_ba =
  (int32, Bigarray.int32_elt, Bigarray.c_layout) Bigarray.Array1.t
type int64_ba =
  (int32, Bigarray.int32_elt, Bigarray.c_layout) Bigarray.Array1.t
type float32_ba =
  (float, Bigarray.float32_elt, Bigarray.c_layout) Bigarray.Array1.t
type float64_ba =
  (float, Bigarray.float64_elt, Bigarray.c_layout) Bigarray.Array1.t

type _ ve =
  | Bool : bool_ba ve
  | Guid : uint8_ba ve
  | Byte : uint8_ba ve
  | Short : int16_ba ve
  | Int : int32_ba ve
  | Long : int64_ba ve
  | Real : float32_ba ve
  | Float : float64_ba ve
  | Char : char_ba ve
  | Symbol : string array ve
  | Timestamp : int64_ba ve
  | Month : int32_ba ve
  | Date : int32_ba ve
  | Timespan : int64_ba ve
  | Minute : int32_ba ve
  | Second : int32_ba ve
  | Time : int32_ba ve
  | Datetime : float64_ba ve

let int_of_ve : type a. a ve -> int = function
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

let bool_vect = Bool
let guid_vect = Guid
let byte_vect = Byte
let short_vect = Short
let int_vect = Int
let long_vect = Long
let real_vect = Real
let float_vect = Float
let char_vect = Char
let symbol_vect = Symbol
let timestamp_vect = Timestamp
let month_vect = Month
let date_vect = Date
let timespan_vect = Timespan
let minute_vect = Minute
let second_vect = Second
let time_vect = Time
let datetime_vect = Datetime

type (_,_) eq = Eq : ('a,'a) eq

let eq_ve : type a b. a ve -> b ve -> (a,b) eq option = fun a b ->
  match a, b with
  | Bool, Bool -> Some Eq
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
  | Datetime, Datetime -> Some Eq
  | _ -> None

type vector = Vect : 'a ve * 'a -> vector

let create_bool_vect v = Vect (bool_vect, v)
let create_guid_vect v = Vect (guid_vect, v)
let create_byte_vect v = Vect (byte_vect, v)
let create_short_vect v = Vect (short_vect, v)
let create_int_vect v = Vect (int_vect, v)
let create_long_vect v = Vect (long_vect, v)
let create_real_vect v = Vect (real_vect, v)
let create_float_vect v = Vect (float_vect, v)
let create_char_vect v = Vect (char_vect, v)
let create_symbol_vect v = Vect (symbol_vect, v)
let create_timestamp_vect v = Vect (timestamp_vect, v)
let create_month_vect v = Vect (month_vect, v)
let create_date_vect v = Vect (date_vect, v)
let create_timespan_vect v = Vect (timespan_vect, v)
let create_minute_vect v = Vect (minute_vect, v)
let create_second_vect v = Vect (second_vect, v)
let create_time_vect v = Vect (time_vect, v)
let create_datetime_vect v = Vect (datetime_vect, v)

let get_vector :
  type a. a ve -> vector -> a option = fun a (Vect(b,x)) ->
  match eq_ve a b with
  | None -> None
  | Some Eq -> Some x

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
  | Timestamp of int64
  | Month     of int32
  | Date      of int32
  | Timespan  of int64
  | Minute    of int32
  | Second    of int32
  | Time      of int32
  | Datetime  of float

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

external kG_bool : k -> bool_ba = "kG_stub"
let kG_bool k =
  let r = kG_bool k in
  Gc.finalise_last (fun () -> r0 k) r ;
  r
external kG_char : k -> char_ba = "kG_stub"
let kG_char k =
  let r = kG_char k in
  Gc.finalise_last (fun () -> r0 k) r ;
  r
external kG : k -> uint8_ba = "kG_stub"
let kG k =
  let r = kG k in
  Gc.finalise_last (fun () -> r0 k) r ;
  r
external kH : k -> int16_ba = "kH_stub"
let kH k =
  let r = kH k in
  Gc.finalise_last (fun () -> r0 k) r ;
  r
external kI : k -> int32_ba = "kI_stub"
let kI k =
  let r = kI k in
  Gc.finalise_last (fun () -> r0 k) r ;
  r
external kJ : k -> int64_ba = "kJ_stub"
let kJ k =
  let r = kJ k in
  Gc.finalise_last (fun () -> r0 k) r ;
  r
external kE : k -> float32_ba = "kE_stub"
let kE k =
  let r = kE k in
  Gc.finalise_last (fun () -> r0 k) r ;
  r
external kF : k -> float64_ba = "kF_stub"
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
  | Timestamp ts -> ktimestamp ts
  | Timespan ts  -> ktimespan ts
  | Month m      -> kmonth m
  | Date i       -> kd i
  | Minute i     -> kminute i
  | Second i     -> ksecond i
  | Time i       -> kt i
  | Datetime f   -> kz f

and pack = function
  | Atom a -> pack_atom a
  | Vector v when get_vector bool_vect v <> None -> begin
    match get_vector bool_vect v with
    | None -> assert false
    | Some v ->
      let len = Int64.of_int (Bigarray.Array1.dim v) in
      let k = ktn (int_of_ve bool_vect) len in
      let ba = kG_bool k in
      Bigarray.Array1.blit v ba ;
      k
  end
  | Vector v when get_vector guid_vect v <> None -> begin
    match get_vector guid_vect v with
    | None -> assert false
    | Some v ->
      let len = Int64.of_int (Bigarray.Array1.dim v) in
      let k = ktn (int_of_ve guid_vect) len in
      let ba = kG k in
      Bigarray.Array1.blit v ba ;
      k
  end
  | Vector v when get_vector byte_vect v <> None -> begin
    match get_vector byte_vect v with
    | None -> assert false
    | Some v ->
      let len = Int64.of_int (Bigarray.Array1.dim v) in
      let k = ktn (int_of_ve byte_vect) len in
      let ba = kG k in
      Bigarray.Array1.blit v ba ;
      k
  end
  | Vector v when get_vector short_vect v <> None -> begin
    match get_vector short_vect v with
    | None -> assert false
    | Some v ->
      let len = Int64.of_int (Bigarray.Array1.dim v) in
      let k = ktn (int_of_ve short_vect) len in
      let ba = kH k in
      Bigarray.Array1.blit v ba ;
      k
  end
  | Vector v when get_vector int_vect v <> None -> begin
    match get_vector int_vect v with
    | None -> assert false
    | Some v ->
      let len = Int64.of_int (Bigarray.Array1.dim v) in
      let k = ktn (int_of_ve int_vect) len in
      let ba = kI k in
      Bigarray.Array1.blit v ba ;
      k
  end
  | Vector v when get_vector long_vect v <> None -> begin
    match get_vector long_vect v with
    | None -> assert false
    | Some v ->
      let len = Int64.of_int (Bigarray.Array1.dim v) in
      let k = ktn (int_of_ve long_vect) len in
      let ba = kJ k in
      Bigarray.Array1.blit v ba ;
      k
  end
  | Vector v when get_vector real_vect v <> None -> begin
    match get_vector real_vect v with
    | None -> assert false
    | Some v ->
      let len = Int64.of_int (Bigarray.Array1.dim v) in
      let k = ktn (int_of_ve real_vect) len in
      let ba = kE k in
      Bigarray.Array1.blit v ba ;
      k
  end
  | Vector v when get_vector float_vect v <> None -> begin
    match get_vector float_vect v with
    | None -> assert false
    | Some v ->
      let len = Int64.of_int (Bigarray.Array1.dim v) in
      let k = ktn (int_of_ve float_vect) len in
      let ba = kF k in
      Bigarray.Array1.blit v ba ;
      k
  end
  | Vector v when get_vector char_vect v <> None -> begin
    match get_vector char_vect v with
    | None -> assert false
    | Some v ->
      let len = Int64.of_int (Bigarray.Array1.dim v) in
      let k = ktn (int_of_ve char_vect) len in
      let ba = kG_char k in
      Bigarray.Array1.blit v ba ;
      k
  end
  | Vector v when get_vector symbol_vect v <> None -> begin
    match get_vector symbol_vect v with
    | None -> assert false
    | Some v ->
      let len = Int64.of_int (Array.length v) in
      let k = ktn (int_of_ve symbol_vect) len in
      Array.iteri (kS_set k) v ;
      k
  end
  | Vector v when get_vector timestamp_vect v <> None -> begin
    match get_vector timestamp_vect v with
    | None -> assert false
    | Some v ->
      let len = Int64.of_int (Bigarray.Array1.dim v) in
      let k = ktn (int_of_ve timestamp_vect) len in
      let ba = kJ k in
      Bigarray.Array1.blit v ba ;
      k
  end
  | Vector v when get_vector month_vect v <> None -> begin
    match get_vector month_vect v with
    | None -> assert false
    | Some v ->
      let len = Int64.of_int (Bigarray.Array1.dim v) in
      let k = ktn (int_of_ve month_vect) len in
      let ba = kI k in
      Bigarray.Array1.blit v ba ;
      k
  end
  | Vector v when get_vector date_vect v <> None -> begin
    match get_vector date_vect v with
    | None -> assert false
    | Some v ->
      let len = Int64.of_int (Bigarray.Array1.dim v) in
      let k = ktn (int_of_ve date_vect) len in
      let ba = kI k in
      Bigarray.Array1.blit v ba ;
      k
  end
  | Vector v when get_vector timespan_vect v <> None -> begin
    match get_vector timespan_vect v with
    | None -> assert false
    | Some v ->
      let len = Int64.of_int (Bigarray.Array1.dim v) in
      let k = ktn (int_of_ve timespan_vect) len in
      let ba = kJ k in
      Bigarray.Array1.blit v ba ;
      k
  end
  | Vector v when get_vector minute_vect v <> None -> begin
    match get_vector minute_vect v with
    | None -> assert false
    | Some v ->
      let len = Int64.of_int (Bigarray.Array1.dim v) in
      let k = ktn (int_of_ve minute_vect) len in
      let ba = kI k in
      Bigarray.Array1.blit v ba ;
      k
  end
  | Vector v when get_vector second_vect v <> None -> begin
    match get_vector second_vect v with
    | None -> assert false
    | Some v ->
      let len = Int64.of_int (Bigarray.Array1.dim v) in
      let k = ktn (int_of_ve second_vect) len in
      let ba = kI k in
      Bigarray.Array1.blit v ba ;
      k
  end
  | Vector v when get_vector time_vect v <> None -> begin
    match get_vector time_vect v with
    | None -> assert false
    | Some v ->
      let len = Int64.of_int (Bigarray.Array1.dim v) in
      let k = ktn (int_of_ve time_vect) len in
      let ba = kI k in
      Bigarray.Array1.blit v ba ;
      k
  end
  | Vector v -> begin
    match get_vector datetime_vect v with
    | None -> assert false
    | Some v ->
      let len = Int64.of_int (Bigarray.Array1.dim v) in
      let k = ktn (int_of_ve datetime_vect) len in
      let ba = kF k in
      Bigarray.Array1.blit v ba ;
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
  | 12 -> Timestamp (k_j k)
  | 13 -> Month (k_i k)
  | 14 -> Date (k_i k)
  | 16 -> Timespan (k_j k)
  | 17 -> Minute (k_i k)
  | 18 -> Second (k_i k)
  | 19 -> Time (k_i k)
  | 15 -> Datetime (k_f k)
  | _  -> invalid_arg "unpack_atom: not an atom"

and unpack_vector k =
  match k_objtyp k with
  | 1 -> create_bool_vect (kG_bool k)
  | 2 -> create_guid_vect (kG k)
  | 4 -> create_byte_vect (kG k)
  | 5 -> create_short_vect (kH k)
  | 6 -> create_int_vect (kI k)
  | 7 -> create_long_vect (kJ k)
  | 8 -> create_real_vect (kE k)
  | 9 -> create_float_vect (kF k)
  | 10 -> create_char_vect (kG_char k)
  | 11 ->
    let len = Int64.to_int (k_length k) in
    create_symbol_vect (Array.init len (kS k))
  | 12 -> create_timestamp_vect (kJ k)
  | 13 -> create_month_vect (kI k)
  | 14 -> create_date_vect (kI k)
  | 16 -> create_timespan_vect (kJ k)
  | 17 -> create_minute_vect (kI k)
  | 18 -> create_second_vect (kI k)
  | 19 -> create_time_vect (kI k)
  | 15 -> create_datetime_vect (kF k)
  | _ -> invalid_arg "unpack_vector: not a vector"

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
  d9 (pack (Vector (create_char_vect s)))

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
    match get_vector char_vect (unpack_vector r) with
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
