(*---------------------------------------------------------------------------
   Copyright (c) 2018 Vincent Bernardoff. All rights reserved.
   Distributed under the ISC license, see terms at the end of the file.
  ---------------------------------------------------------------------------*)

type k

type t =
  | Atom of atom
  | Vector of atom list
  | List of atom list
  | Dict of t * t
  | Table of t * t
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

let int_of_atom = function
  | Bool _      -> 1
  | Guid _      -> 2
  | Byte _      -> 4
  | Short _     -> 5
  | Int _       -> 6
  | Long _      -> 7
  | Real _      -> 8
  | Float _     -> 9
  | Char _      -> 10
  | Symbol _    -> 11
  | Timestamp _ -> 12
  | Month _     -> 13
  | Date _      -> 14
  | Timespan _  -> 16
  | Minute _    -> 17
  | Second _    -> 18
  | Time _      -> 19
  | Datetime _  -> 15

(* Accessors *)

external k_objtyp : k -> int = "k_objtyp" [@@noalloc]
external k_objattrs : k -> int = "k_objattrs" [@@noalloc]
external k_refcount : k -> int = "k_refcount" [@@noalloc]
external k_length : k -> int = "k_length" [@@noalloc]

external k_g : k -> int = "k_g" [@@noalloc]
external k_h : k -> int = "k_h" [@@noalloc]

external k_i : k -> int32 = "k_i"
external k_j : k -> int64 = "k_j"
external k_e : k -> float = "k_e"
external k_f : k -> float = "k_f"

external k_s : k -> string = "k_s"
external k_u : k -> string = "k_u"
let k_u k =
  match Uuidm.of_bytes (k_u k) with
  | None -> invalid_arg "k_u"
  | Some u -> u

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

external ktn : int -> int -> k = "ktn_stub"
external ja_int : k -> int -> unit = "ja_int_stub"
(* external ja_long : k -> int -> unit = "ja_long_stub" *)
external ja_int32 : k -> int32 -> unit = "ja_int32_stub"
external ja_int64 : k -> int64 -> unit = "ja_int32_stub"
external ja_double : k -> float -> unit = "ja_double_stub"
external ja_bool : k -> bool -> unit = "ja_double_stub"
external ja_uuid : k -> string -> unit = "ja_uuid_stub"
external js : k -> string -> unit = "js_stub"

let pack_atom = function
  | Bool b  -> kb b
  | Guid u -> ku (Uuidm.to_bytes u)
  | Byte i -> kg i
  | Short i -> kh i
  | Int i -> ki i
  | Long j -> kj j
  | Real f -> ke f
  | Float f -> kf f
  | Char c -> kc c
  | Symbol s -> ks s
  | Timestamp ts -> ktimestamp ts
  | Timespan ts -> ktimespan ts
  | Month m -> kmonth m
  | Date i -> kd i
  | Minute i -> kminute i
  | Second i -> ksecond i
  | Time i -> kt i
  | Datetime f -> kz f

let cons k = function
  | Bool b -> ja_bool k b
  | Guid u -> ja_uuid k (Uuidm.to_bytes u)
  | Byte i -> ja_int k i
  | Short i -> ja_int k i
  | Int i -> ja_int32 k i
  | Long j -> ja_int64 k j
  | Real f -> ja_double k f
  | Float f -> ja_double k f
  | Char c -> ja_int k (Char.code c)
  | Symbol s -> js k s
  | Timestamp ts -> ja_int64 k ts
  | Timespan ts -> ja_int64 k ts
  | Month m -> ja_int32 k m
  | Date i -> ja_int32 k i
  | Minute i -> ja_int32 k i
  | Second i -> ja_int32 k i
  | Time i -> ja_int32 k i
  | Datetime f -> ja_double k f

let pack_list = function
  | [] -> ktn 0 0
  | t :: _ as l ->
    let k = ktn (int_of_atom t) 0 in
    List.iter (cons k) l ;
    k

let unpack_bool k =
  match k_objtyp k, k_g k with
  | -1, 0 -> Some false
  | -1, _ -> Some true
  | _ -> None

let unpack_guid k =
  match k_objtyp k with
  | -2 -> Some (k_u k)
  | _ -> None

let unpack_byte k =
  match k_objtyp k with
  | -4 -> Some (k_g k)
  | _ -> None

let unpack_short k =
  match k_objtyp k with
  | -5 -> Some (k_h k)
  | _ -> None

let unpack_int k =
  match k_objtyp k with
  | -6 -> Some (k_i k)
  | _ -> None

let unpack_long k =
  match k_objtyp k with
  | -7 -> Some (k_j k)
  | _ -> None

let unpack_real k =
  match k_objtyp k with
  | -8 -> Some (k_e k)
  | _ -> None

let unpack_float k =
  match k_objtyp k with
  | -9 -> Some (k_f k)
  | _ -> None

let unpack_char k =
  match k_objtyp k with
  | -10 -> Some (Char.chr (k_g k))
  | _ -> None

let unpack_symbol k =
  match k_objtyp k with
  | -11 -> Some (k_s k)
  | _ -> None

let unpack_timestamp k =
  match k_objtyp k with
  | -12 -> Some (k_j k)
  | _ -> None

let unpack_month k =
  match k_objtyp k with
  | -13 -> Some (k_i k)
  | _ -> None

let unpack_date k =
  match k_objtyp k with
  | -13 -> Some (k_i k)
  | _ -> None

let unpack_timespan k =
  match k_objtyp k with
  | -16 -> Some (k_j k)
  | _ -> None

let unpack_date k =
  match k_objtyp k with
  | -17 -> Some (k_i k)
  | _ -> None

let unpack_date k =
  match k_objtyp k with
  | -18 -> Some (k_i k)
  | _ -> None

let unpack_date k =
  match k_objtyp k with
  | -19 -> Some (k_i k)
  | _ -> None

let unpack_datetime k =
  match k_objtyp k with
  | -15 -> Some (k_f k)
  | _ -> None

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
