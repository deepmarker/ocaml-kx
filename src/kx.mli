(*---------------------------------------------------------------------------
   Copyright (c) 2018 Vincent Bernardoff. All rights reserved.
   Distributed under the ISC license, see terms at the end of the file.
  ---------------------------------------------------------------------------*)

type kb (** boolean *)

type uu (** uuid *)

type kg (** byte *)

type kh (** short *)

type ki (** int *)

type kj (** long *)

type ke (** real *)

type kf (** float *)

type kc (** char *)

type ks (** symbol *)

type kp (** timestamp *)

type km (** month *)

type kd (** date *)

type kn (** timespan *)

type ku (** minute *)

type kv (** second *)

type kt (** time *)

type kz (** datetime *)

type k (** K object *)

type _ t =
  | Bool : bool -> kb t
  | Guid : Uuidm.t -> uu t
  | Byte : int -> kg t
  | Short : int -> kh t
  | Int : int32 -> ki t
  | Long : int64 -> kj t
  | Real : float -> ke t
  | Double : float -> kf t
  | Char : char -> kc t
  | Symbol : string -> ks t
  | Timestamp : int64 -> kp t
  | Month : int32 -> km t
  | Date : int32 -> kd t
  | Timespan : int64 -> kn t
  | Minute : int32 -> ku t
  | Second : int32 -> kv t
  | Millisecond : int32 -> kt t
  | Datetime : float -> kz t
(** Parsed K object *)

val pack : _ t -> k
(** [pack t] is the K object serialization of [t]. *)

(* val kG : t -> int option
 * val kC : t -> char option *)

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
