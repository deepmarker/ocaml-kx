type ('ml, 'c) bv =
  ('c, 'ml, Bigarray.c_layout) Bigarray.Array1.t

type g
type h
type i
type j
type e
type f
type u
type s

type time
type date
type datetime
type timestamp
type timespan
type month
type minute
type second

type gv = (int, Bigarray.int8_unsigned_elt) bv
type hv = (int, Bigarray.int16_signed_elt)  bv
type iv = (int32, Bigarray.int32_elt)       bv
type jv = (int64, Bigarray.int64_elt)       bv
type ev = (float, Bigarray.float32_elt)     bv
type fv = (float, Bigarray.float64_elt)     bv

type k

type 'a atom        = k
type ('ml, 'c) vect = k
type 'a list        = k
type ('a, 'b) dict  = k
type ('a, 'b) table = k

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

external kG_bool : k -> gv = "kG_stub"
external kG_char : k -> Bigstring.t = "kG_stub"
external kG : k -> gv = "kG_stub"
external kU : k -> Bigstring.t = "kU_stub"
external kH : k -> hv = "kH_stub"
external kI : k -> iv = "kI_stub"
external kJ : k -> jv = "kJ_stub"
external kE : k -> ev = "kE_stub"
external kF : k -> fv = "kF_stub"

(* Atom constructors *)

external kb : bool -> bool atom = "kb_stub"
external ku : string -> Uuidm.t atom = "ku_stub"

external kc : char -> char atom = "kc_stub"
external kg : int -> g atom = "kg_stub"
external kh : int -> h atom = "kh_stub"
external ki : int32 -> i atom = "ki_stub"
external kj : int64 -> j atom = "kj_stub"

external ke : float -> e atom = "ke_stub"
external kf : float -> f atom = "kf_stub"

external ks : string -> s atom = "ks_stub"

external kt : int -> time atom = "kt_stub"
external kd : int -> date atom = "kd_stub"
external kmonth : int -> month atom = "kmonth_stub"
external kminute : int -> minute atom = "kminute_stub"
external ksecond : int -> second atom = "ksecond_stub"

external ktimestamp : int64 -> timestamp atom = "ktimestamp_stub"
external ktimespan : int64 -> timespan atom = "ktimespan_stub"
external kz : float -> datetime atom = "kz_stub"

(* List constructors *)

external ktn : int -> int -> k = "ktn_stub"

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

module T = struct
  let b = 1
  let x = 4
  let h = 5
  let i = 6
  let j = 7
  let e = 8
  let f = 9
  let c = 10
  let s = 11
  let p = 12
  let m = 13
  let d = 14
  let z = 15
  let n = 16
  let u = 17
  let v = 18
  let t = 19

  let boolean = b
  let byte = x
  let short = h
  let int = i
  let long = j
  let real = e
  let float = f
  let char = c
  let symbol = s
  let timestamp = p
  let month = m
  let date = d
  let datetime = z
  let timespan = n
  let minute = u
  let second = v
  let time = t
end

let bool = kb

let bool_vect bv =
  kG
