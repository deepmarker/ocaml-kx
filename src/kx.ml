(*---------------------------------------------------------------------------
   Copyright (c) 2018 Vincent Bernardoff. All rights reserved.
   Distributed under the ISC license, see terms at the end of the file.
  ---------------------------------------------------------------------------*)

type k
type t =
  | Bool of bool
  | Guid of Uuidm.t
  | Byte of int
  | Short of int
  | Int of int32
  | Long of int64
  | Real of float
  | Double of float
  | Char of char
  | Symbol of string
  | Timestamp of int64
  | Month of int32
  | Date of int32
  | Timespan of int64
  | Minute of int32
  | Second of int32
  | Millisecond of int32
  | Datetime of float

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

let pack = function
  | Bool b -> kb b
  | Guid u -> ku (Uuidm.to_bytes u)
  | Byte i -> kg i
  | Short i -> kh i
  | Int i -> ki i
  | Long j -> kj j
  | Real f -> ke f
  | Double f -> kf f
  | Char c -> kc c
  | Symbol s -> ks s
  | Timestamp ts -> ktimestamp ts
  | Timespan ts -> ktimespan ts
  | Month m -> kmonth m
  | Date i -> kd i
  | Minute i -> kminute i
  | Second i -> ksecond i
  | Millisecond i -> kt i
  | Datetime f -> kz f

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
