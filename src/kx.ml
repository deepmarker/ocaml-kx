(*---------------------------------------------------------------------------
   Copyright (c) 2018 Vincent Bernardoff. All rights reserved.
   Distributed under the ISC license, see terms at the end of the file.
  ---------------------------------------------------------------------------*)

type kb (* boolean *)
type uu (* uuid *)
type kg (* byte *)
type kh (* short *)
type ki (* int *)
type kj (* long *)
type ke (* real *)
type kf (* float *)
type kc (* char *)
type ks (* symbol *)

type kp (* timestamp *)
type km (* month *)
type kd (* date *)

type kn (* timespan *)
type ku (* minute *)
type kv (* second *)
type kt (* time *)

type kz (* datetime *)

type k
type _ t =
  | Bool : bool -> kb t
  | Guid : Uuidm.t -> uu t
  | Byte : int -> kg t
  | Short : int -> kh t
  | Int : int32 -> ki t
  | Long : int64 -> kj t
  | Real : float -> ke t
  | Float : float -> kf t
  | Char : char -> kc t
  | Symbol : string -> ks t
  | Timestamp : int64 -> kp t
  | Month : int32 -> km t
  | Date : int32 -> kd t
  | Timespan : int64 -> kn t
  | Minute : int32 -> ku t
  | Second : int32 -> kv t
  | Time : int32 -> kt t
  | Datetime : float -> kz t

let inttype_of_t : type a. a t -> int = function
  | Bool _ -> 1
  | Guid _ -> 2
  | Byte _ -> 4
  | Short _ -> 5
  | Int _ -> 6
  | Long _ -> 7
  | Real _ -> 8
  | Float _ -> 9
  | Char _ -> 10
  | Symbol _ -> 11
  | Timestamp _ -> 12
  | Month _ -> 13
  | Date _ -> 14
  | Timespan _ -> 16
  | Minute _ -> 17
  | Second _ -> 18
  | Time _ -> 19
  | Datetime _ -> 15

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

let pack : type a. a t -> k = function
  | Bool b -> kb b
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

let cons : type a. k -> a t -> unit = fun k -> function
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
  | h :: _ as l ->
    let k = ktn (inttype_of_t h) 0 in
    List.iter (cons k) l ;
    k

(* external kG : t -> int = "kG_stub" [@@noalloc] *)

(* let kC t = Char.chr (kG t) *)

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
