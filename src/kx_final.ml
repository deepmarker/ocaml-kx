(* Type of bigarrays. *)

type ('ml, 'c) bv =
  ('ml, 'c, Bigarray.c_layout) Bigarray.Array1.t

type gv = (int, Bigarray.int8_unsigned_elt) bv
type hv = (int, Bigarray.int16_signed_elt)  bv
type iv = (int32, Bigarray.int32_elt)       bv
type jv = (int64, Bigarray.int64_elt)       bv
type ev = (float, Bigarray.float32_elt)     bv
type fv = (float, Bigarray.float64_elt)     bv

type time = { time : Ptime.time ; ms : int }
type timespan = { time : Ptime.time ; ns : int}

module W = struct
  type _ atom =
    | Boolean : bool atom
    | Guid : Uuidm.t atom
    | Byte : char atom
    | Short : int atom
    | Int : int32 atom
    | Long : int64 atom
    | Real : float atom
    | Float : float atom
    | Char : char atom
    | Symbol : string atom
    | Timestamp : Ptime.t atom
    | Month : Ptime.date atom
    | Date : Ptime.date atom
    | Timespan : timespan atom
    | Minute : Ptime.time atom
    | Second : Ptime.time atom
    | Time : time atom

  (* OCaml type of a K object. *)
  type _ t =
    | Atom : 'a atom -> 'a t
    | Vect : 'a atom -> 'a array t
    | String : char atom -> string t
    | Nil : unit t
    | Cons : 'a t * 'b t -> ('a * 'b) t
    | Dict : 'a t * 'b t -> ('a * 'b) t
    | Table : 'a t * 'b t -> ('a * 'b) t
    | Conv : ('a -> 'b t) * ('b t -> 'a) * 'a -> 'b t

  let bool = Atom Boolean
  let guid = Atom Guid
  let byte = Atom Byte
  let short = Atom Short
  let int = Atom Int
  let long = Atom Long
  let real = Atom Real
  let float = Atom Float
  let char = Atom Char
  let sym = Atom Symbol
  let timestamp = Atom Timestamp
  let month = Atom Month
  let date = Atom Date
  let timespan = Atom Timespan
  let minute = Atom Minute
  let second = Atom Second
  let time = Atom Time

  let nil = Nil
  let cons a b = Cons (a, b)
  let dict k v = Dict (k, v)
  let table k v = Table (k, v)

  let conv project inject a =
    Conv (project, inject, a)
end

type k
(* OCaml handler to a K object *)

type _ t = K : 'a W.t * k -> 'a t

let k (K (_, k)) = k

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
external kz : float -> k = "kz_stub"

(* List constructors *)

external ktn : int -> int -> k = "ktn_stub" (* [ktn type length] *)
external js : k -> string -> unit = "js_stub"
external jk : k -> k -> unit = "jk_stub"
external jv : k -> k -> unit = "jv_stub"

(* Dict/Table accessors *)

external xD : k -> k -> k = "xD_stub"
external xT : k -> k = "xT_stub"

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

(* Utilities *)

external ymd : int -> int -> int -> int = "ymd_stub" [@@noalloc]
external dj : int -> int = "dj_stub" [@@noalloc]

let int_of_time ((h, m, s), tz_offset) ms =
  (h - tz_offset) * 3600 + m * 60 + s * 1_000 + ms

let int64_of_timespan ((h, m, s), tz_offset) ns =
  let open Int64 in
  add
    (mul (of_int ((h - tz_offset) * 3600 + m * 60 + s)) 1_000_000_000L)
    (of_int ns)

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

let int_of_month (y, m, _) = (y - 2000) * 12 + m

let int_of_minute ((hh, mm, _), tz_offset) = (hh * 60 + mm + tz_offset / 60)
let int_of_second ((hh, mm, ss), tz_offset) =
  (hh * 3600 + mm * 60 + ss + tz_offset)

let string_of_chars a =
  String.init (Array.length a) (Array.get a)

let rec append_list : type a b. a W.t -> a -> b t -> (a * b) t = fun w a (K (bw, k)) ->
  let K (_, k') = construct w a in
  jv k k' ;
  K (Cons (w, bw), k)

and construct : type a. a W.t -> a -> a t = fun w a ->
  match w with
  | Nil -> K (Nil, ktn 0 0)
  | Cons (aw, bw) ->
    let x, y = a in
    let b = construct bw y in
    append_list aw x b

  | Dict (k, v) ->
    let x, y = a in
    let K (_, k') = construct k x in
    let K (_, k'') = construct v y in
    K (Dict (k, v), xD k' k'')

  | Table (k, v) ->
    let x, y = a in
    let K (_, k') = construct k x in
    let K (_, k'') = construct v y in
    K (Table (k, v), xT (xD k' k''))

  | Atom Boolean -> K (Atom Boolean, kb a)
  | Atom Byte -> K (Atom Byte, kg a)
  | Atom Short -> K (Atom Short, kh a)
  | Atom Int -> K (Atom Int, ki a)
  | Atom Long -> K (Atom Long, kj a)
  | Atom Real -> K (Atom Real, ke a)
  | Atom Float -> K (Atom Float, kf a)
  | Atom Char -> K (Atom Char, kc a)
  | Atom Symbol -> K (Atom Symbol, ks a)
  | Atom Guid -> K (Atom Guid, ku (Uuidm.to_bytes a))
  | Atom Date -> let y, m, d = a in K (Atom Date, (kd (ymd y m d)))
  | Atom Time -> let { time ; ms } = a in K (Atom Time, (kt (int_of_time time ms)))
  | Atom Timespan -> let { time ; ns } = a in K( Atom Timespan, (ktimespan (int64_of_timespan time ns)))
  | Atom Timestamp -> K (Atom Timestamp, (ktimestamp (int64_of_timestamp a)))
  | Atom Month -> K (Atom Month, (kmonth (int_of_month a)))
  | Atom Minute -> K (Atom Minute, (kminute (int_of_minute a)))
  | Atom Second -> K (Atom Second, (kminute (int_of_second a)))

  | Vect Boolean ->
    let k = ktn 1 (Array.length a) in
    Array.iteri begin fun i -> function
      | false -> Bigarray.Array1.set (kG k) i 0
      | true -> Bigarray.Array1.set (kG k) i 1
    end a ;
    K (Vect Boolean, k)

  | String Byte ->
    let len = String.length a in
    let k = ktn 4 len in
    Bigstring.blit_of_string a 0 (kG_char k) 0 len ;
    K (String Byte, k)

  | Vect Byte ->
    let a = string_of_chars a in
    let len = String.length a in
    let k = ktn 4 len in
    Bigstring.blit_of_string a 0 (kG_char k) 0 len ;
    K (Vect Byte, k)

  | String Char ->
    let len = String.length a in
    let k = ktn 10 len in
    Bigstring.blit_of_string a 0 (kG_char k) 0 len ;
    K (String Char, k)

  | Vect Char ->
    let a = string_of_chars a in
    let len = String.length a in
    let k = ktn 10 len in
    Bigstring.blit_of_string a 0 (kG_char k) 0 len ;
    K (Vect Char, k)

  | Vect Short ->
    let k = ktn 5 (Array.length a) in
    Array.iteri (Bigarray.Array1.set (kH k)) a ;
    K (Vect Short, k)

  | Vect Int ->
    let k = ktn 6 (Array.length a) in
    Array.iteri (Bigarray.Array1.set (kI k)) a ;
    K (Vect Int, k)

  | Vect Long ->
    let k = ktn 7 (Array.length a) in
    Array.iteri (Bigarray.Array1.set (kJ k)) a ;
    K (Vect Long, k)

  | Vect Real ->
    let k = ktn 8 (Array.length a) in
    Array.iteri (Bigarray.Array1.set (kE k)) a ;
    K (Vect Real, k)

  | Vect Float ->
    let k = ktn 9 (Array.length a) in
    Array.iteri (Bigarray.Array1.set (kF k)) a ;
    K (Vect Float, k)

  | Vect Symbol ->
    let k = ktn 11 (Array.length a) in
    Array.iteri (kS_set k) a ;
    K (Vect Symbol, k)

  | Vect Guid ->
    let len = Array.length a in
    let k = ktn 2 len in
    let buf = kU k in
    Array.iteri begin fun i u ->
      Bigstring.blit_of_string (Uuidm.to_bytes u) 0 buf (i*16) 16
    end a ;
    K (Vect Guid, k)

  | Vect Timestamp ->
    let k = ktn 12 (Array.length a) in
    Array.iteri (fun i a -> Bigarray.Array1.set (kJ k) i (int64_of_timestamp a)) a ;
    K (Vect Timestamp, k)

  | Vect Month ->
    let k = ktn 13 (Array.length a) in
    Array.iteri (fun i a -> Bigarray.Array1.set (kI k) i (Int32.of_int (int_of_month a))) a ;
    K (Vect Month, k)

  | Vect Date ->
    let k = ktn 14 (Array.length a) in
    Array.iteri (fun i (y,m,d) -> Bigarray.Array1.set (kI k) i (Int32.of_int (ymd y m d))) a ;
    K (Vect Date, k)

  | Vect Timespan ->
    let k = ktn 16 (Array.length a) in
    Array.iteri (fun i { time ; ns } -> Bigarray.Array1.set (kJ k) i (int64_of_timespan time ns)) a ;
    K (Vect Timespan, k)

  | Vect Minute ->
    let k = ktn 17 (Array.length a) in
    Array.iteri (fun i a -> Bigarray.Array1.set (kI k) i (Int32.of_int (int_of_minute a))) a ;
    K (Vect Minute, k)

  | Vect Second ->
    let k = ktn 18 (Array.length a) in
    Array.iteri (fun i a -> Bigarray.Array1.set (kI k) i (Int32.of_int (int_of_second a))) a ;
    K (Vect Second, k)

  | Vect Time ->
    let k = ktn 19 (Array.length a) in
    Array.iteri (fun i { time ; ms } -> Bigarray.Array1.set (kI k) i (Int32.of_int (int_of_time time ms))) a ;
    K (Vect Time, k)

  | Conv (project, _, v) ->
    let w = project v in
    construct w a

  | String _ -> assert false

let destruct : type a. a W.t -> k -> a option = fun _w _a -> None

