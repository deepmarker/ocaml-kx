(*---------------------------------------------------------------------------
   Copyright (c) 2018 Vincent Bernardoff. All rights reserved.
   Distributed under the ISC license, see terms at the end of the file.
  ---------------------------------------------------------------------------*)

type k
(** Type of K objects in memory *)

type bool_ba =
  (bool, Bigarray.int8_unsigned_elt, Bigarray.c_layout) Bigarray.Array1.t
type char_ba =
  (char, Bigarray.int8_unsigned_elt, Bigarray.c_layout) Bigarray.Array1.t
type uint8_ba =
  (int, Bigarray.int8_unsigned_elt, Bigarray.c_layout) Bigarray.Array1.t
type int16_ba =
  (int, Bigarray.int16_signed_elt, Bigarray.c_layout) Bigarray.Array1.t
type int32_ba =
  (int32, Bigarray.int32_elt, Bigarray.c_layout) Bigarray.Array1.t
type int64_ba =
  (int32, Bigarray.int32_elt, Bigarray.c_layout) Bigarray.Array1.t
type float32_ba =
  (float, Bigarray.float32_elt, Bigarray.c_layout) Bigarray.Array1.t
type float64_ba =
  (float, Bigarray.float64_elt, Bigarray.c_layout) Bigarray.Array1.t

type _ ve
val bool_vect : bool_ba ve
val guid_vect : Bigstring.t ve
val byte_vect : uint8_ba ve
val short_vect : int16_ba ve
val int_vect : int32_ba ve
val long_vect : int64_ba ve
val real_vect : float32_ba ve
val float_vect : float64_ba ve
val char_vect : char_ba ve
val symbol_vect : string array ve
val timestamp_vect : int64_ba ve
val month_vect : int32_ba ve
val date_vect : int32_ba ve
val timespan_vect : int64_ba ve
val minute_vect : int32_ba ve
val second_vect : int32_ba ve
val time_vect : int32_ba ve
val datetime_vect : float64_ba ve
(** Values representing types of vector elements. *)

type vector
(** Type of a vector. *)

val get_vector : 'a ve -> vector -> 'a option
(** [get_vector typ v] is the underlying storage of the vector
    value. *)

type t =
  | Atom      of atom
  | Vector    of vector
  | List      of t array
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
  | Timestamp of int64
  | Month     of int32
  | Date      of int32
  | Timespan  of int64
  | Minute    of int32
  | Second    of int32
  | Time      of int32
  | Datetime  of float

(* val pack : t -> k
 * (\** [pack t] packs [t] into a K object. *\) *)

val unpack : k -> t
(** [unpack k] is the OCaml representation of [k], a K object. *)

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
