(*---------------------------------------------------------------------------
   Copyright (c) 2018 Vincent Bernardoff. All rights reserved.
   Distributed under the ISC license, see terms at the end of the file.
  ---------------------------------------------------------------------------*)

type k
(** Type of K objects in memory *)

type _ atom =
  | Bool      : bool atom
  | Guid      : Uuidm.t atom
  | Byte      : int atom
  | Short     : int atom
  | Int       : int32 atom
  | Long      : int64 atom
  | Real      : float atom
  | Float     : float atom
  | Char      : char atom
  | Symbol    : string atom
  | Timestamp : int64 atom
  | Month     : int32 atom
  | Date      : int32 atom
  | Timespan  : int64 atom
  | Minute    : int32 atom
  | Second    : int32 atom
  | Time      : int32 atom
  | Datetime  : float atom

type dyn

type _ t =
  | Atom : 'a atom * 'a -> 'a t
  | Vector : ('a atom * 'a) list -> 'a t
  | List : dyn list -> dyn t
  | Dict : 'a t * 'b t -> ('a * 'b) t
  | Table : 'a t * 'b t -> ('a * 'b) t

val pack : 'a atom -> 'a -> k
(** [pack t] packs [t] into a K object. *)

val pack_list : ('a atom * 'a) list -> k
(** [pack_list ts] packs [ts] into a K object. *)

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
