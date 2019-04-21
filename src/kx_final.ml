(* Type of bigarrays. *)

type ('ml, 'c) bv =
  ('ml, 'c, Bigarray.c_layout) Bigarray.Array1.t

type gv = (int, Bigarray.int8_unsigned_elt) bv
type hv = (int, Bigarray.int16_signed_elt)  bv
type iv_int = (int, Bigarray.int_elt)       bv
type iv = (int32, Bigarray.int32_elt)       bv
type jv = (int64, Bigarray.int64_elt)       bv
type ev = (float, Bigarray.float32_elt)     bv
type fv = (float, Bigarray.float64_elt)     bv

type time = { time : Ptime.time ; ms : int }
type timespan = { time : Ptime.time ; ns : int}

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

type (_, _) eq = Eq : ('a, 'a) eq

let eq : type a b. a typ -> b typ -> (a, b) eq option = fun a b ->
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

(* OCaml type of a K object. *)
type _ w =
  | Atom : 'a typ -> 'a w
  | Vect : 'a typ -> 'a array w
  | String : char typ -> string w
  | Nil : unit w
  | Cons : 'a w * 'b w -> ('a * 'b) w
  | Dict : 'a w * 'b w -> ('a * 'b) w
  | Table : 'a w * 'b w -> ('a * 'b) w
  | Conv : ('a -> 'b) * ('b -> 'a) * 'b w -> 'a w

let rec equal : type a b. a w -> b w -> bool = fun a b ->
  match a, b with
  | Atom a, Atom b -> eq a b <> None
  | Vect a, Vect b -> eq a b <> None
  | String a, String b -> eq a b <> None
  | Nil, Nil -> true
  | Cons (a, b), Cons (c, d) -> equal a c && equal b d
  | Dict (a, b), Dict (c, d) -> equal a c && equal b d
  | Table (a, b), Table (c, d) -> equal a c && equal b d
  | Conv (_, _, a), Conv (_, _, b) -> equal a b
  | _ -> false

let pp ppf _ = Format.pp_print_string ppf "<abstract>"

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

let atom a = Atom a
let vect a = Vect a
let string a = String a

let nil = Nil
let cons a b = Cons (a, b)

let tup1 a = Cons (a, Nil)
let tup2 a b = Cons (a, cons b nil)

let dict k v = Dict (k, v)
let table k v = Table (k, v)

let conv project inject a =
  Conv (project, inject, a)

type k
(* OCaml handler to a K object *)

type t = K : 'a w * k -> t

let equal (K (w1, _)) (K (w2, _)) = equal w1 w2
let pp ppf (K (w, _)) = pp ppf w

(* Accessors *)

external k_objtyp : k -> int = "k_objtyp" [@@noalloc]
(* external k_objattrs : k -> int = "k_objattrs" [@@noalloc]
 * external k_refcount : k -> int = "k_refcount" [@@noalloc] *)
external k_length : k -> int = "k_length" [@@noalloc]
(* external k_length64 : k -> int64 = "k_length64" *)

external k_g : k -> int = "k_g" [@@noalloc]
external k_h : k -> int = "k_h" [@@noalloc]
external k_ii : k -> int = "k_i_int" [@@noalloc]

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

(* List accessors: memory obtained is valid as long as K object is not
   GCed. *)

external kK : k -> int -> k = "kK_stub"
external kK_set : k -> int -> k -> unit = "kK_set_stub"
external kS : k -> int -> string = "kS_stub"
external kS_set : k -> int -> string -> unit = "kS_set_stub"

external kG_char : k -> Bigstring.t = "kG_stub"
external kG : k -> gv = "kG_stub"
external kU : k -> Bigstring.t = "kU_stub"
external kH : k -> hv = "kH_stub"
external kII : k -> iv_int = "kI_int_stub"
external kI : k -> iv = "kI_stub"
external kJ : k -> jv = "kJ_stub"
external kE : k -> ev = "kE_stub"
external kF : k -> fv = "kF_stub"

(* Atom constructors *)

external kb : bool -> k = "kb_stub"
external ku : string -> k = "ku_stub"

external kc : char -> k = "kc_stub"
external kg : char -> k = "kg_stub"
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
(* external kz : float -> k = "kz_stub" *)

(* List constructors *)

external ktn : int -> int -> k = "ktn_stub" (* [ktn type length] *)
(* external js : k -> string -> unit = "js_stub" *)
(* external jk : k -> k -> unit = "jk_stub" *)
(* external jv : k -> k -> unit = "jv_stub" *)

(* Dict/Table accessors *)

external xD : k -> k -> k = "xD_stub"
external xT : k -> k = "xT_stub"

(* Utilities *)

external ymd : int -> int -> int -> int = "ymd_stub" [@@noalloc]
external dj : int -> int = "dj_stub" [@@noalloc]

let date_of_int d =
  let date = dj d in
  let y = date / 10_000 in
  let m = (date mod 10_000) / 100 in
  let d = (date mod 10_000) mod 100 in
  (y, m, d)

let int_of_time ((h, m, s), tz_offset) ms =
  (h - tz_offset) * 3600 + m * 60 + s * 1_000 + ms

let time_of_int time =
  let ms = time mod 1_000 in
  let s = time / 1_000 in
  let hh = s / 3600 in
  let mm = (s / 60) mod 60 in
  let ss = s mod 60 in
  { time = ((hh, mm, ss), 0); ms }

let int64_of_timespan ((h, m, s), tz_offset) ns =
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

(* J pu(J u){return 1000000LL*(u-10957LL*86400000LL);} // kdb+
   timestamp from unix, use ktj(Kj,n) to create timestamp from n *)
let int64_of_timestamp ts =
  let span_since_kxepoch =
    Ptime.(Span.sub (to_span ts) kx_epoch_span) in
  let d, ps = Ptime.Span.to_d_ps span_since_kxepoch in
  Int64.(add (day_in_ns d) (div ps 1_000L))

let timestamp_of_int64 nanos_since_kxepoch =
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

let int_of_month (y, m, _) = (y - 2000) * 12 + m
let month_of_int m =
  let y = m / 12 in
  let rem_m = m mod 12 in
  2000 + y, rem_m, 0

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

let rec construct_list :
  type a. k list -> a w -> a -> k list = fun ks w a ->
  match w with
  | Nil -> ks
  | Cons (hw, tw) ->
    let h, t = a in
    let K (_, k) = construct hw h in
    construct_list (k :: ks) tw t
  | _ -> assert false

and construct : type a. a w -> a -> t = fun w a ->
  match w with
  | Nil -> K (w, ktn 0 0)
  | Cons _ ->
    let ks = construct_list [] w a in
    let len = List.length ks in
    let k = ktn 0 len in
    List.iteri (fun i v -> kK_set k (len-1-i) v) ks ;
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
    K (w, xT (xD k' k''))

  | Conv (project, _, v) -> construct v (project a)
  | Atom Boolean -> K (w, kb a)
  | Atom Byte -> K (w, kg a)
  | Atom Short -> K (w, kh a)
  | Atom Int -> K (w, ki a)
  | Atom Long -> K (w, kj a)
  | Atom Real -> K (w, ke a)
  | Atom Float -> K (w, kf a)
  | Atom Char -> K (w, kc a)
  | Atom Symbol -> K (w, ks a)
  | Atom Guid -> K (w, ku (Uuidm.to_bytes a))
  | Atom Date -> let y, m, d = a in K (w, (kd (ymd y m d)))
  | Atom Time -> let { time ; ms } = a in K (w, (kt (int_of_time time ms)))
  | Atom Timespan -> let { time ; ns } = a in K (w, (ktimespan (int64_of_timespan time ns)))
  | Atom Timestamp -> K (w, (ktimestamp (int64_of_timestamp a)))
  | Atom Month -> K (w, (kmonth (int_of_month a)))
  | Atom Minute -> K (w, (kminute (int_of_minute a)))
  | Atom Second -> K (w, (ksecond (int_of_second a)))

  | Vect Boolean ->
    let k = ktn 1 (Array.length a) in
    Array.iteri begin fun i -> function
      | false -> Bigarray.Array1.set (kG k) i 0
      | true -> Bigarray.Array1.set (kG k) i 1
    end a ;
    K (w, k)

  | String Byte ->
    let len = String.length a in
    let k = ktn 4 len in
    Bigstring.blit_of_string a 0 (kG_char k) 0 len ;
    K (w, k)

  | Vect Byte ->
    let a = string_of_chars a in
    let len = String.length a in
    let k = ktn 4 len in
    Bigstring.blit_of_string a 0 (kG_char k) 0 len ;
    K (w, k)

  | String Char ->
    let len = String.length a in
    let k = ktn 10 len in
    Bigstring.blit_of_string a 0 (kG_char k) 0 len ;
    K (w, k)

  | Vect Char ->
    let a = string_of_chars a in
    let len = String.length a in
    let k = ktn 10 len in
    Bigstring.blit_of_string a 0 (kG_char k) 0 len ;
    K (w, k)

  | Vect Short ->
    let k = ktn 5 (Array.length a) in
    Array.iteri (Bigarray.Array1.set (kH k)) a ;
    K (w, k)

  | Vect Int ->
    let k = ktn 6 (Array.length a) in
    Array.iteri (Bigarray.Array1.set (kI k)) a ;
    K (w, k)

  | Vect Long ->
    let k = ktn 7 (Array.length a) in
    Array.iteri (Bigarray.Array1.set (kJ k)) a ;
    K (w, k)

  | Vect Real ->
    let k = ktn 8 (Array.length a) in
    Array.iteri (Bigarray.Array1.set (kE k)) a ;
    K (w, k)

  | Vect Float ->
    let k = ktn 9 (Array.length a) in
    Array.iteri (Bigarray.Array1.set (kF k)) a ;
    K (w, k)

  | Vect Symbol ->
    let k = ktn 11 (Array.length a) in
    Array.iteri (kS_set k) a ;
    K (w, k)

  | Vect Guid ->
    let k = ktn 2 (Array.length a) in
    let buf = kU k in
    Array.iteri begin fun i u ->
      Bigstring.blit_of_string (Uuidm.to_bytes u) 0 buf (i*16) 16
    end a ;
    K (w, k)

  | Vect Timestamp ->
    let k = ktn 12 (Array.length a) in
    Array.iteri (fun i a -> Bigarray.Array1.set (kJ k) i (int64_of_timestamp a)) a ;
    K (w, k)

  | Vect Month ->
    let k = ktn 13 (Array.length a) in
    Array.iteri (fun i a -> Bigarray.Array1.set (kII k) i (int_of_month a)) a ;
    K (w, k)

  | Vect Date ->
    let k = ktn 14 (Array.length a) in
    Array.iteri (fun i (y,m,d) -> Bigarray.Array1.set (kII k) i (ymd y m d)) a ;
    K (w, k)

  | Vect Timespan ->
    let k = ktn 16 (Array.length a) in
    Array.iteri (fun i { time ; ns } -> Bigarray.Array1.set (kJ k) i (int64_of_timespan time ns)) a ;
    K (w, k)

  | Vect Minute ->
    let k = ktn 17 (Array.length a) in
    Array.iteri (fun i a -> Bigarray.Array1.set (kII k) i (int_of_minute a)) a ;
    K (w, k)

  | Vect Second ->
    let k = ktn 18 (Array.length a) in
    Array.iteri (fun i a -> Bigarray.Array1.set (kII k) i (int_of_second a)) a ;
    K (w, k)

  | Vect Time ->
    let k = ktn 19 (Array.length a) in
    Array.iteri (fun i { time ; ms } -> Bigarray.Array1.set (kII k) i (int_of_time time ms)) a ;
    K (w, k)

  | String _ -> assert false

let rec depack_list : type a. a w -> int -> k -> a option = fun w i k ->
  match w with
  | Nil -> Some ()
  | Cons (h, t) -> begin
      match destruct h (kK k i) with
      | None -> None
      | Some v -> match depack_list t (succ i) k with
        | None -> None
        | Some vv -> Some (v, vv)
    end
  | _ -> assert false

and destruct : type a. a w -> k -> a option = fun w k ->
  match w with
  | Nil -> Some ()
  | Cons _ when k_objtyp k = 0 -> depack_list w 0 k

  | Table _ when k_objtyp k = 98 -> destruct w (k_k k)
  | Dict (kw, vw) when k_objtyp k = 99 -> begin
      match destruct kw (kK k 0), destruct vw (kK k 1) with
      | Some a, Some b -> Some (a, b)
      | _ -> None
    end

  | Atom Boolean when k_objtyp k = -1 -> Some (k_g k <> 0)
  | Atom Guid when k_objtyp k = -2 -> Some (k_u k)
  | Atom Byte when k_objtyp k = -4 -> Some (Char.chr (k_g k))
  | Atom Short when k_objtyp k = -5 -> Some (k_h k)
  | Atom Int when k_objtyp k = -6 -> Some (k_i k)
  | Atom Long when k_objtyp k = -7 -> Some (k_j k)
  | Atom Real when k_objtyp k = -8 -> Some (k_e k)
  | Atom Float when k_objtyp k = -9 -> Some (k_f k)
  | Atom Char when k_objtyp k = -10 -> Some (Char.chr (k_g k))
  | Atom Symbol when k_objtyp k = -11 -> Some (k_s k)
  | Atom Timestamp when k_objtyp k = -12 -> Some (timestamp_of_int64 (k_j k))
  | Atom Month when k_objtyp k = -13 -> Some (month_of_int (k_ii k))
  | Atom Date when k_objtyp k = -14 -> Some (date_of_int (k_ii k))
  | Atom Timespan when k_objtyp k = -16 -> Some (timespan_of_int64 (k_j k))
  | Atom Minute when k_objtyp k = -17 -> Some (minute_of_int (k_ii k))
  | Atom Second when k_objtyp k = -18 -> Some (second_of_int (k_ii k))
  | Atom Time when k_objtyp k = -19 -> Some (time_of_int (k_ii k))

  | Vect Boolean when k_objtyp k = 1 ->
    let len = k_length k in
    let buf = kG_char k in
    Some (Array.init len (fun i -> Bigstring.get buf i <> '\x00'))

  | Vect Guid when k_objtyp k = 2 -> Some (guids (kU k))

  | Vect Byte when k_objtyp k = 4 ->
    let len = k_length k in
    let buf = kG_char k in
    Some (Array.init len (Bigstring.get buf))

  | String Byte when k_objtyp k = 4 ->
    let buf = kG_char k in
    Some (Bigstring.to_string buf)

  | Vect Short when k_objtyp k = 5 ->
    let len = k_length k in
    let buf = kH k in
    Some (Array.init len (Bigarray.Array1.get buf))

  | Vect Int when k_objtyp k = 6 ->
    let len = k_length k in
    let buf = kI k in
    Some (Array.init len (Bigarray.Array1.get buf))

  | Vect Long when k_objtyp k = 7 ->
    let len = k_length k in
    let buf = kJ k in
    Some (Array.init len (Bigarray.Array1.get buf))

  | Vect Real when k_objtyp k = 8 ->
    let len = k_length k in
    let buf = kE k in
    Some (Array.init len (Bigarray.Array1.get buf))

  | Vect Float when k_objtyp k = 9 ->
    let len = k_length k in
    let buf = kF k in
    Some (Array.init len (Bigarray.Array1.get buf))

  | Vect Char when k_objtyp k = 10 ->
    let len = k_length k in
    let buf = kG_char k in
    Some (Array.init len (Bigstring.get buf))

  | String Char when k_objtyp k = 10 ->
    let buf = kG_char k in
    Some (Bigstring.to_string buf)

  | Vect Symbol when k_objtyp k = 11 ->
    let len = k_length k in
    Some (Array.init len (kS k))

  | Vect Timestamp when k_objtyp k = 12 ->
    let len = k_length k in
    let buf = kJ k in
    Some (Array.init len (fun i -> timestamp_of_int64 (Bigarray.Array1.get buf i)))

  | Vect Month when k_objtyp k = 13 ->
    let len = k_length k in
    let buf = kII k in
    Some (Array.init len (fun i -> month_of_int (Bigarray.Array1.get buf i)))

  | Vect Date when k_objtyp k = 14 ->
    let len = k_length k in
    let buf = kII k in
    Some (Array.init len (fun i -> date_of_int (Bigarray.Array1.get buf i)))

  | Vect Timespan when k_objtyp k = 16 ->
    let len = k_length k in
    let buf = kJ k in
    Some (Array.init len (fun i -> timespan_of_int64 (Bigarray.Array1.get buf i)))

  | Vect Minute when k_objtyp k = 17 ->
    let len = k_length k in
    let buf = kII k in
    Some (Array.init len (fun i -> minute_of_int (Bigarray.Array1.get buf i)))

  | Vect Second when k_objtyp k = 18 ->
    let len = k_length k in
    let buf = kII k in
    Some (Array.init len (fun i -> second_of_int (Bigarray.Array1.get buf i)))

  | Vect Time when k_objtyp k = 19 ->
    let len = k_length k in
    let buf = kII k in
    Some (Array.init len (fun i -> time_of_int (Bigarray.Array1.get buf i)))

  | _ -> None

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

let destruct w (K (_, k)) = destruct w k
