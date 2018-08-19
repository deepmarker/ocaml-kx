(*---------------------------------------------------------------------------
   Copyright (c) 2018 Vincent Bernardoff. All rights reserved.
   Distributed under the ISC license, see terms at the end of the file.
  ---------------------------------------------------------------------------*)

type k

type t =
  | Atom of atom
  | Vector of atom array
  | List of atom array
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

let int_of_t = function
  | Atom a -> ~- (int_of_atom a)
  | Vector [||] ->
    invalid_arg "int_of_t: cannot know type of empty vector"
  | Vector v -> int_of_atom v.(0)
  | List _ -> 0
  | Dict _ -> 99
  | Table _ -> 98

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
external k_u : k -> string = "k_u"
let k_u k =
  match Uuidm.of_bytes (k_u k) with
  | None -> invalid_arg "k_u"
  | Some u -> u

(* List accessors *)

external kK : k -> int -> k = "kK_stub"
external kU : k -> int -> string = "kU_stub"
let kU k i =
  match Uuidm.of_bytes (kU k i) with
  | None -> invalid_arg "kU"
  | Some u -> u
external kG : k -> int -> int = "kG_stub" [@@noalloc]
external kH : k -> int -> int = "kH_stub" [@@noalloc]
external kI : k -> int -> int32 = "kI_stub"
external kJ : k -> int -> int64 = "kJ_stub"
external kE : k -> int -> float = "kE_stub"
external kF : k -> int -> float = "kF_stub"
external kS : k -> int -> string = "kS_stub"

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

external ktn : int -> int -> k = "ktn_stub"

(* Dict/Table accessors *)

external xD : k -> k -> k = "xD_stub"
external xT : k -> k = "xT_stub"
external ktd : k -> k = "ktd_stub"

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

let append k = function
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
    List.iter (append k) l ;
    k

let unpack_atom k =
  match ~- (k_objtyp k) with
  | 1 -> (match k_g k with 0 -> Bool false | _ -> Bool true)
  | 2 -> Guid (k_u k)
  | 4 -> Byte (k_g k)
  | 5 -> Short (k_h k)
  | 6 -> Int (k_i k)
  | 7 -> Long (k_j k)
  | 8 -> Real (k_e k)
  | 9 -> Float (k_f k)
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
  | _ -> invalid_arg "unpack_atom: not an atom"

let unpack_vector k =
  let objtyp = k_objtyp k in
  if objtyp < 1 then
    invalid_arg "unpack_vector: argument is not a vector" ;
  let len = Int64.to_int (k_length k) in
  Array.init len begin fun i ->
    match objtyp with
    | 1 -> Bool (kG k i <> 0)
    | 2 -> Guid (kU k i)
    | 4 -> Byte (kG k i)
    | 5 -> Short (kH k i)
    | 6 -> Int (kI k i)
    | 7 -> Long (kJ k i)
    | 8 -> Real (kE k i)
    | 9 -> Float (kF k i)
    | 10 -> Char (Char.chr (kG k i))
    | 11 -> Symbol (kS k i)
    | 12 -> Timestamp (kJ k i)
    | 13 -> Month (kI k i)
    | 14 -> Date (kI k i)
    | 16 -> Timespan (kJ k i)
    | 17 -> Minute (kI k i)
    | 18 -> Second (kI k i)
    | 19 -> Time (kI k i)
    | 15 -> Datetime (kF k i)
    | _ -> invalid_arg "unpack_vector: unknown type"
  end

let unpack_list k =
  if k_objtyp k <> 0 then
    invalid_arg "unpack_list: argument is not a mixed list" ;
  ()

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
