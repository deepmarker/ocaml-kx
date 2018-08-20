(*---------------------------------------------------------------------------
   Copyright (c) 2018 Vincent Bernardoff. All rights reserved.
   Distributed under the ISC license, see terms at the end of the file.
  ---------------------------------------------------------------------------*)

type k
(** Type of K objects in memory *)

val k_objtyp : k -> int
(** Type of K object *)

val k_objattrs : k -> int
(** Attributes of K object (0 = no attributes) *)

val k_refcount : k -> int
(** Reference count of K object *)

val k_length : k -> int64
(** Number of elements in a list *)

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
val guid_vect : uint8_ba ve
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

val create_bool_vect : bool_ba -> vector
val create_guid_vect : uint8_ba -> vector
val create_byte_vect : uint8_ba -> vector
val create_short_vect : int16_ba -> vector
val create_int_vect : int32_ba -> vector
val create_long_vect : int64_ba -> vector
val create_real_vect : float32_ba -> vector
val create_float_vect : float64_ba -> vector
val create_char_vect : char_ba -> vector
val create_symbol_vect : string array -> vector
val create_timestamp_vect : int64_ba -> vector
val create_month_vect : int32_ba -> vector
val create_date_vect : int32_ba -> vector
val create_timespan_vect : int64_ba -> vector
val create_minute_vect : int32_ba -> vector
val create_second_vect : int32_ba -> vector
val create_time_vect : int32_ba -> vector
val create_datetime_vect : float64_ba -> vector
(** Vector constructors *)

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

val pack : t -> k
(** [pack t] packs [t] into a K object. *)

val unpack : k -> t
(** [unpack k] is the OCaml representation of [k], a K object. *)

val bigstring_of_k : k -> char_ba option
(** [bigstring_of_k k] is [Some ba] iff [k] is a char vector. *)

val of_bigstring : char_ba -> (k, string) result
val of_bigstring_exn : char_ba -> k

val serialize : ?mode:int -> k -> (char_ba, string) result

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
