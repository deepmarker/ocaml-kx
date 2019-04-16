(*---------------------------------------------------------------------------
   Copyright (c) 2018 Vincent Bernardoff. All rights reserved.
   Distributed under the ISC license, see terms at the end of the file.
  ---------------------------------------------------------------------------*)

type k

type ('a, 'b) storage = ('a, 'b, Bigarray.c_layout) Bigarray.Array1.t

type uint8_arr   = (int, Bigarray.int8_unsigned_elt) storage
type int16_arr   = (int, Bigarray.int16_signed_elt) storage
type int32_arr   = (int32, Bigarray.int32_elt) storage
type int64_arr   = (int64, Bigarray.int64_elt) storage
type float32_arr = (float, Bigarray.float32_elt) storage
type float64_arr = (float, Bigarray.float64_elt) storage

let guid_arr a =
  let buf = Bigstring.create (16 * Array.length a) in
  Array.iteri begin fun i guid ->
    Bigstring.blit_of_string (Uuidm.to_bytes guid) 0 buf (i * 16) 16
  end a ;
  buf

let guids_of_arr a =
  let len = Bigstring.length a in
  if len mod 16 <> 0 then
    invalid_arg ("guids_of_arr: " ^  string_of_int len) ;
  let nb_guids =  len / 16 in
  let res = ref [] in
  for i = nb_guids - 1 downto 0 do
    let guid = Bigstring.sub_string a (i*16) 16 in
    match Uuidm.of_bytes guid with
    | None -> assert false
    | Some v -> res := v :: !res
  done ;
  !res

let bool_arr a =
  let a = Array.map (function true -> 1 | false -> 0) a in
  Bigarray.(Array1.of_array int8_unsigned c_layout) a

let kx_epoch, kx_epoch_span =
  match Ptime.of_date (2000, 1, 1) with
  | None -> assert false
  | Some t -> t, Ptime.to_span t

let day_in_ns d =
  Int64.(mul (of_int (d * 24 * 3600)) 1_000_000_000L)

let int64_of_timestamp ts =
  let span_since_kxepoch =
    Ptime.(Span.sub (to_span ts) kx_epoch_span) in
  let d, ps = Ptime.Span.to_d_ps span_since_kxepoch in
  Int64.(add (day_in_ns d) (div ps 1_000L))

let timestamp_arr ts =
  let len = Array.length ts in
  let buf = Bigarray.(Array1.create int64 c_layout len) in
  Array.iteri begin fun i t ->
    Bigarray.Array1.unsafe_set buf i (int64_of_timestamp t)
  end ts ;
  buf

type _ kw =
  | Bool      : uint8_arr kw
  | Guid      : Bigstring.t kw
  | Byte      : uint8_arr kw
  | Short     : int16_arr kw
  | Int       : int32_arr kw
  | Long      : int64_arr kw
  | Real      : float32_arr kw
  | Float     : float64_arr kw
  | Char      : Bigstring.t kw
  | Symbol    : string list kw
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

let equal_vect (Vect (w1, a)) (Vect (w2, b)) =
  match eq_kw w1 w2 with
  | None -> false
  | Some Eq ->
    match w1 with
    | Guid -> Bigstring.equal a b
    | Char -> Bigstring.equal a b
    | Real ->
      let len_a = Bigarray.Array1.dim a in
      let len_b = Bigarray.Array1.dim a in
      if len_a <> len_b then false
      else begin
        try
          for i = 0 to len_a - 1 do
            let ai = Bigarray.Array1.unsafe_get a i in
            let bi = Bigarray.Array1.unsafe_get b i in
            if not (Float.equal ai bi) then
              raise Exit
          done ;
          true
        with Exit -> false
      end
    | Float ->
      let len_a = Bigarray.Array1.dim a in
      let len_b = Bigarray.Array1.dim a in
      if len_a <> len_b then false
      else begin
        try
          for i = 0 to len_a - 1 do
            let ai = Bigarray.Array1.unsafe_get a i in
            let bi = Bigarray.Array1.unsafe_get b i in
            if not (Float.equal ai bi) then
              raise Exit
          done ;
          true
        with Exit -> false
      end
    | _ -> a = b

module Vect = struct
  let bool       v = Vect (bool, v)
  let guid       v = Vect (guid, v)
  let byte       v = Vect (byte, v)
  let short      v = Vect (short, v)
  let int        v = Vect (int, v)
  let long       v = Vect (long, v)
  let real       v = Vect (real, v)
  let float      v = Vect (float, v)
  let char       v = Vect (char, v)
  let symbol     v = Vect (symbol, v)
  let timestamp  v = Vect (timestamp, v)
  let month      v = Vect (month, v)
  let date       v = Vect (date, v)
  let timespan   v = Vect (timespan, v)
  let minute     v = Vect (minute, v)
  let second     v = Vect (second, v)
  let time       v = Vect (time, v)
  let datetime   v = Vect (datetime, v)
end

let get_vector :
  type a. a kw -> vector -> a option = fun a (Vect(b,x)) ->
  match eq_kw a b with
  | None -> None
  | Some Eq -> Some x

let vector_is kw v =
  match get_vector kw v with
  | None -> false
  | Some _ -> true

let pp_print_vector ppf v =
  let pp_sep ppf () = Format.fprintf ppf " " in
  let list_of_ba ?(f=fun a -> a) ba =
    let len = Bigarray.Array1.dim ba in
    let res = ref [] in
    for i = len - 1 downto 0 do
      res := f (Bigarray.Array1.unsafe_get ba i) :: !res
    done ;
    !res in
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
      match get_vector float v with
      | Some v ->
        Format.fprintf ppf "`float$(%a)"
          Format.(pp_print_list ~pp_sep pp_print_float) (list_of_ba v)
      | None ->
        match get_vector symbol v with
        | Some syms ->
          Format.fprintf ppf "`symbol$(%a)"
            Format.(pp_print_list ~pp_sep pp_print_string) syms
        | None ->
          Format.pp_print_string ppf "Vector <abstract>"

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

external kG_bool : k -> uint8_arr = "kG_stub"
external kG_char : k -> Bigstring.t = "kG_stub"
external kG : k -> uint8_arr = "kG_stub"
external kU : k -> Bigstring.t = "kU_stub"
external kH : k -> int16_arr = "kH_stub"
external kI : k -> int32_arr = "kI_stub"
external kJ : k -> int64_arr = "kJ_stub"
external kE : k -> float32_arr = "kE_stub"
external kF : k -> float64_arr = "kF_stub"

let kG_bool k =
  let r = kG_bool k in
  Gc.finalise_last (fun () -> r0 k) r ;
  r

let kG_char k =
  let r = kG_char k in
  Gc.finalise_last (fun () -> r0 k) r ;
  r

let kG k =
  let r = kG k in
  Gc.finalise_last (fun () -> r0 k) r ;
  r

let kU k =
  let r = kU k in
  Gc.finalise_last (fun () -> r0 k) r ;
  r

let kH k =
  let r = kH k in
  Gc.finalise_last (fun () -> r0 k) r ;
  r

let kI k =
  let r = kI k in
  Gc.finalise_last (fun () -> r0 k) r ;
  r

let kJ k =
  let r = kJ k in
  Gc.finalise_last (fun () -> r0 k) r ;
  r

let kE k =
  let r = kE k in
  Gc.finalise_last (fun () -> r0 k) r ;
  r

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

external kt : int -> k = "kt_stub"
external kd : int -> k = "kd_stub"
external kmonth : int -> k = "kmonth_stub"
external kminute : int -> k = "kminute_stub"
external ksecond : int -> k = "ksecond_stub"

external ktimestamp : int64 -> k = "ktimestamp_stub"
external ktimespan : int64 -> k = "ktimespan_stub"
external kz : float -> k = "kz_stub"

(* List constructors *)

external ktn : int -> int -> k = "ktn_stub"

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

external ymd : int -> int -> int -> int = "ymd_stub" [@@noalloc]
external dj : int -> int = "dj_stub" [@@noalloc]

type time = { time: Ptime.time ; ms: int }
type timespan = { time: Ptime.time ; ns: int }

type t =
  | Atom      of atom
  | Vector    of vector
  | List      of t list
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
  | Date      of Ptime.date
  | Time      of time
  | Timespan  of timespan
  | Minute    of Ptime.time
  | Second    of Ptime.time
  | Datetime  of float

let equal_atom a b = match a, b with
  | Guid a, Guid b -> Uuidm.equal a b
  | Timestamp a, Timestamp b -> Ptime.equal a b
  | _ -> a = b

let rec equal t1 t2 =
  match t1, t2 with
  | Atom a, Atom b -> equal_atom a b
  | Vector v1, Vector v2 -> equal_vect v1 v2
  | List a, List b ->
    List.length a = List.length b &&
    List.fold_left2 (fun a x y -> a && equal x y) true a b
  | Dict (k, v), Dict (k2, v2) -> equal k v && equal k2 v2
  | Table (k, v), Table(k2, v2) -> equal k v && equal k2 v2
  | _ -> false

let pp_print_atom ppf = function
  | Bool b -> Format.pp_print_bool ppf b
  | Guid b -> Uuidm.pp_string ppf b
  | Byte g -> Format.pp_print_int ppf g
  | Short h -> Format.pp_print_int ppf h
  | Int i -> Format.fprintf ppf "%ld" i
  | Long j -> Format.fprintf ppf "%Ld" j
  | Real r -> Format.fprintf ppf "%g" r
  | Float f -> Format.fprintf ppf "%g" f
  | Char c -> Format.pp_print_char ppf c
  | Symbol s -> Format.pp_print_string ppf s
  | Timestamp t -> Ptime.pp_rfc3339 () ppf t
  | _ -> Format.pp_print_string ppf "<abstract>"

let rec pp ppf v =
  let pp_sep ppf () = Format.fprintf ppf " " in
  match v with
  | Atom a -> pp_print_atom ppf a
  | Vector v -> pp_print_vector ppf v
  | List vs ->
    Format.fprintf ppf "(%a)" (Format.pp_print_list ~pp_sep pp) vs
  | Dict (k, v) ->
    Format.fprintf ppf "{ k:%a; v:%a }" pp k pp v
  | Table (k, v) ->
    Format.fprintf ppf "{| k:%a; v:%a |}" pp k pp v

let atom a = Atom a
let vector v = Vector v
let list l = List l
let dict k v = Dict (k, v)
let table k v = Table (k, v)

let create_timespan ?(tz_offset=0) ~hh ~mm ~ss ~ns () =
  Timespan { time = ((hh, mm, ss), tz_offset) ; ns }

let zero_timespan = create_timespan ~hh:0 ~mm:0 ~ss:0 ~ns:0 ()

module Atom = struct
  let bool b = Atom (Bool b)
  let guid g = Atom (Guid g)
  let byte b = Atom (Byte b)
  let short h = Atom (Short h)
  let int i = Atom (Int i)
  let long j = Atom (Long j)
  let real r = Atom (Real r)
  let float f = Atom (Float f)
  let char c = Atom (Char c)
  let symbol s = Atom (Symbol s)
  let timestamp t = Atom (Timestamp t)
  let month i = Atom (Month i)
  let date d = Atom (Date d)
  let minute d = Atom (Minute d)
  let second d = Atom (Second d)
  let time t = Atom (Time t)
  let timespan t = Atom (Timespan t)
end

let uint8_arr = Bigarray.(Array1.of_array int8_unsigned c_layout)
let int16_arr = Bigarray.(Array1.of_array int16_signed c_layout)
let int32_arr = Bigarray.(Array1.of_array int32 c_layout)
let int64_arr = Bigarray.(Array1.of_array int64 c_layout)
let float32_arr = Bigarray.(Array1.of_array float32 c_layout)
let float64_arr = Bigarray.(Array1.of_array float64 c_layout)

let int_of_time { time = ((h, m, s), tz_offset); ms } =
  (h - tz_offset) * 3600 + m * 60 + s * 1_000 + ms

let int64_of_timespan { time = ((h, m, s), tz_offset) ; ns } =
  let open Int64 in
  add
    (mul (of_int ((h - tz_offset) * 3600 + m * 60 + s)) 1_000_000_000L)
    (of_int ns)

let int_of_minute ((hh, mm, _), tz_offset) = (hh * 60 + mm + tz_offset / 60)
let int_of_second ((hh, mm, ss), tz_offset) =
  (hh * 3600 + mm * 60 + ss + tz_offset)

module VectArray = struct
  let guid gs = Vector (Vect.guid (guid_arr gs))
  let bool bs =  Vector (Vect.bool (bool_arr bs))
  let byte bs = Vector (Vect.byte (uint8_arr bs))
  let short bs = Vector (Vect.short (int16_arr bs))
  let int bs = Vector (Vect.int (int32_arr bs))
  let long bs = Vector (Vect.long (int64_arr bs))
  let real bs = Vector (Vect.real (float32_arr bs))
  let float bs = Vector (Vect.float (float64_arr bs))
  let char bs = Vector (Vect.char (Bigstring.of_string bs))
  let symbol bs = Vector (Vect.symbol (Array.to_list bs))
  let timestamp bs = Vector (Vect.timestamp (timestamp_arr bs))
  let month bs = Vector (Vect.month (int32_arr (Array.map Int32.of_int bs)))
  let date bs =
    let a = Array.map (fun (y, m, d) -> Int32.of_int (ymd y m d)) bs in
    Vector (Vect.date (int32_arr a))
  let time bs =
    let a = Array.map (fun t -> Int32.of_int (int_of_time t)) bs in
    Vector (Vect.time (int32_arr a))
  let timespan bs =
    let a = Array.map int64_of_timespan bs in
    Vector (Vect.timespan (int64_arr a))
  let minute bs =
    let a = Array.map (fun t -> Int32.of_int (int_of_minute t)) bs in
    Vector (Vect.time (int32_arr a))
  let second bs =
    let a = Array.map (fun t -> Int32.of_int (int_of_second t)) bs in
    Vector (Vect.time (int32_arr a))
  let datetime bs = Vector (Vect.datetime (float64_arr bs))
end

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
  | Timestamp ts -> ktimestamp (int64_of_timestamp ts)
  | Time t -> kt (int_of_time t)
  | Timespan t -> ktimespan (int64_of_timespan t)
  | Month i -> kmonth i
  | Date (y, m, d) -> kd (ymd y m d)
  | Minute t -> kminute (int_of_minute t)
  | Second t -> ksecond (int_of_second t)
  | Datetime f   -> kz f

and pack = function
  | Atom a -> pack_atom a
  | Dict (k, v) -> xD (pack k) (pack v)
  | Table (k, v) -> xT (xD (pack k) (pack v))
  | List ts ->
    let len = List.length ts in
    let k = ktn 0 len in
    List.iteri (fun i t -> kK_set k i (pack t)) ts ;
    k
  | Vector v when vector_is bool v -> begin
      match get_vector bool v with
      | None -> assert false
      | Some v ->
        let len = Bigarray.Array1.dim v in
        let k = ktn (int_of_kw bool) len in
        let arr = kG_bool k in
        Bigarray.Array1.blit v arr ;
        k
    end
  | Vector v when vector_is guid v -> begin
      match get_vector guid v with
      | None -> assert false
      | Some v ->
        let len = Bigarray.Array1.dim v / 16 in
        let k = ktn (int_of_kw guid) len in
        let arr = kU k in
        Bigarray.Array1.blit v arr ;
        k
    end
  | Vector v when vector_is byte v -> begin
      match get_vector byte v with
      | None -> assert false
      | Some v ->
        let len = Bigarray.Array1.dim v in
        let k = ktn (int_of_kw byte) len in
        let arr = kG k in
        Bigarray.Array1.blit v arr ;
        k
    end
  | Vector v when vector_is short v -> begin
      match get_vector short v with
      | None -> assert false
      | Some v ->
        let len = Bigarray.Array1.dim v in
        let k = ktn (int_of_kw short) len in
        let arr = kH k in
        Bigarray.Array1.blit v arr ;
        k
    end
  | Vector v when vector_is int v -> begin
      match get_vector int v with
      | None -> assert false
      | Some v ->
        let len = Bigarray.Array1.dim v in
        let k = ktn (int_of_kw int) len in
        let arr = kI k in
        Bigarray.Array1.blit v arr ;
        k
    end
  | Vector v when vector_is long v -> begin
      match get_vector long v with
      | None -> assert false
      | Some v ->
        let len = Bigarray.Array1.dim v in
        let k = ktn (int_of_kw long) len in
        let arr = kJ k in
        Bigarray.Array1.blit v arr ;
        k
    end
  | Vector v when vector_is real v -> begin
      match get_vector real v with
      | None -> assert false
      | Some v ->
        let len = Bigarray.Array1.dim v in
        let k = ktn (int_of_kw real) len in
        let arr = kE k in
        Bigarray.Array1.blit v arr ;
        k
    end
  | Vector v when vector_is float v -> begin
      match get_vector float v with
      | None -> assert false
      | Some v ->
        let len = Bigarray.Array1.dim v in
        let k = ktn (int_of_kw float) len in
        let arr = kF k in
        Bigarray.Array1.blit v arr ;
        k
    end
  | Vector v when vector_is char v -> begin
      match get_vector char v with
      | None -> assert false
      | Some v ->
        let len = Bigarray.Array1.dim v in
        let k = ktn (int_of_kw char) len in
        let arr = kG_char k in
        Bigarray.Array1.blit v arr ;
        k
    end
  | Vector v when vector_is symbol v -> begin
      match get_vector symbol v with
      | None -> assert false
      | Some v ->
        let len = List.length v in
        let k = ktn (int_of_kw symbol) len in
        List.iteri (kS_set k) v ;
        k
    end
  | Vector v when vector_is timestamp v -> begin
      match get_vector timestamp v with
      | None -> assert false
      | Some v ->
        let len = Bigarray.Array1.dim v in
        let k = ktn (int_of_kw timestamp) len in
        let arr = kJ k in
        Bigarray.Array1.blit v arr ;
        k
    end
  | Vector v when vector_is month v -> begin
      match get_vector month v with
      | None -> assert false
      | Some v ->
        let len = Bigarray.Array1.dim v in
        let k = ktn (int_of_kw month) len in
        let arr = kI k in
        Bigarray.Array1.blit v arr ;
        k
    end
  | Vector v when vector_is date v -> begin
      match get_vector date v with
      | None -> assert false
      | Some v ->
        let len = Bigarray.Array1.dim v in
        let k = ktn (int_of_kw date) len in
        let arr = kI k in
        Bigarray.Array1.blit v arr ;
        k
    end
  | Vector v when vector_is timespan v -> begin
      match get_vector timespan v with
      | None -> assert false
      | Some v ->
        let len = Bigarray.Array1.dim v in
        let k = ktn (int_of_kw timespan) len in
        let arr = kJ k in
        Bigarray.Array1.blit v arr ;
        k
    end
  | Vector v when vector_is minute v -> begin
      match get_vector minute v with
      | None -> assert false
      | Some v ->
        let len = Bigarray.Array1.dim v in
        let k = ktn (int_of_kw minute) len in
        let arr = kI k in
        Bigarray.Array1.blit v arr ;
        k
    end
  | Vector v when vector_is second v -> begin
      match get_vector second v with
      | None -> assert false
      | Some v ->
        let len = Bigarray.Array1.dim v in
        let k = ktn (int_of_kw second) len in
        let arr = kI k in
        Bigarray.Array1.blit v arr ;
        k
    end
  | Vector v when vector_is time v -> begin
      match get_vector time v with
      | None -> assert false
      | Some v ->
        let len = Bigarray.Array1.dim v in
        let k = ktn (int_of_kw time) len in
        let arr = kI k in
        Bigarray.Array1.blit v arr ;
        k
    end
  | Vector v -> begin
      match get_vector datetime v with
      | None -> assert false
      | Some v ->
        let len = Bigarray.Array1.dim v in
        let k = ktn (int_of_kw datetime) len in
        let arr = kF k in
        Bigarray.Array1.blit v arr ;
        k
    end

let rec unpack_atom k =
  match -(k_objtyp k) with
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
  | 14 ->
    let date = dj (Int32.to_int (k_i k)) in
    let y = date / 10_000 in
    let m = (date mod 10_000) / 100 in
    let d = (date mod 10_000) mod 100 in
    Date (y, m, d)
  | 16 ->
    let time = k_j k in
    let ns = Int64.(to_int (rem time 1_000_000_000L)) in
    let s = Int64.(to_int (div time 1_000_000_000L)) in
    let hh = s / 3600 in
    let mm = (s / 60) mod 60 in
    let ss = s mod 60 in
    Timespan { time = (hh, mm, ss), 0 ; ns }
  | 17 ->
    let nb_minutes = Int32.to_int (k_i k) in
    Minute ((nb_minutes / 60, nb_minutes mod 60, 0), 0)
  | 18 ->
    let nb_seconds = Int32.to_int (k_i k) in
    let hh = nb_seconds / 3600 in
    let mm = (nb_seconds / 60) mod 60 in
    let ss = nb_seconds mod 60 in
    Second ((hh, mm, ss), 0)
  | 19 ->
    let time = Int32.to_int (k_i k) in
    let ms = time mod 1_000 in
    let s = time / 1_000 in
    let hh = s / 3600 in
    let mm = (s / 60) mod 60 in
    let ss = s mod 60 in
    Time { time = ((hh, mm, ss), 0); ms }
  | 15 -> Datetime (k_f k)
  | _  -> invalid_arg "unpack_atom: not an atom"

and unpack_vector k =
  match k_objtyp k with
  | 1  -> Vect.bool (kG_bool k)
  | 2  -> Vect.guid (kU k)
  | 4  -> Vect.byte (kG k)
  | 5  -> Vect.short (kH k)
  | 6  -> Vect.int (kI k)
  | 7  -> Vect.long (kJ k)
  | 8  -> Vect.real (kE k)
  | 9  -> Vect.float (kF k)
  | 10 -> Vect.char (kG_char k)
  | 11 ->
    let len = Int64.to_int (k_length k) in
    let res = ref [] in
    for i = len - 1 downto 0 do
      res := kS k i :: !res
    done ;
    Vect.symbol (!res)
  | 12 -> Vect.timestamp (kJ k)
  | 13 -> Vect.month (kI k)
  | 14 -> Vect.date (kI k)
  | 16 -> Vect.timespan (kJ k)
  | 17 -> Vect.minute (kI k)
  | 18 -> Vect.second (kI k)
  | 19 -> Vect.time (kI k)
  | 15 -> Vect.datetime (kF k)
  | _  -> invalid_arg "unpack_vector: not a vector"

and unpack_list k =
  let len = Int64.to_int (k_length k) in
  let res = ref [] in
  for i = len - 1 downto 0 do
    let kk = kK k i in
    res := unpack kk :: !res
  done ;
  !res

and unpack_dict k =
  unpack (kK k 0), unpack (kK k 1)

and unpack_table k = unpack_dict (k_k k)

and unpack k =
  match k_objtyp k with
  | 0            -> List (unpack_list k)
  | n when n < 0 -> Atom (unpack_atom k)
  | 99           -> let k, v = unpack_dict k in dict k v
  | 98           -> let k, v = unpack_table k in table k v
  | _            -> Vector (unpack_vector k)

let ktrue = pack (Atom (Bool true))
let kfalse = pack (Atom (Bool false))

(* Serialization *)

external b9 : int -> k -> (k, string) result = "b9_stub"
external d9 : k -> (k, string) result = "d9_stub"

let of_bigstring s =
  d9 (pack (Vector (Vect.char s)))

let of_bigstring_exn s =
  match (of_bigstring s) with
  | Error msg -> invalid_arg msg
  | Ok s -> s

let to_bigstring ?(mode = ~-1) k =
  match b9 mode k with
  | Error e -> failwith e
  | Ok r ->
    match get_vector byte (unpack_vector r) with
    | None -> assert false
    | Some bs -> (Obj.magic bs : Bigstring.t)

external khp : string -> int -> int = "khp_stub"
external khpu : string -> int -> string -> int = "khpu_stub"
external khpun : string -> int -> string -> int -> int = "khpun_stub"
external khpunc : string -> int -> string -> int -> int -> int = "khpunc_stub"
external kclose : Unix.file_descr -> unit = "kclose_stub" [@@noalloc]

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
