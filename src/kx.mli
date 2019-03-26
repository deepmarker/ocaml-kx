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

type ('a, 'b) storage = ('a, 'b, Bigarray.c_layout) Bigarray.Array1.t

type bool_arr    = (bool, Bigarray.int8_unsigned_elt) storage
type char_arr    = (char, Bigarray.int8_unsigned_elt) storage
type uint8_arr   = (int, Bigarray.int8_unsigned_elt) storage
type int16_arr   = (int, Bigarray.int16_signed_elt) storage
type int32_arr   = (int32, Bigarray.int32_elt) storage
type int64_arr   = (int32, Bigarray.int32_elt) storage
type float32_arr = (float, Bigarray.float32_elt) storage
type float64_arr = (float, Bigarray.float64_elt) storage

type _ kw
(** Type witness for k types. *)

val bool      : bool_arr kw
val guid      : uint8_arr kw
val byte      : uint8_arr kw
val short     : int16_arr kw
val int       : int32_arr kw
val long      : int64_arr kw
val real      : float32_arr kw
val float     : float64_arr kw
val char      : char_arr kw
val symbol    : string array kw
val timestamp : int64_arr kw
val month     : int32_arr kw
val date      : int32_arr kw
val timespan  : int64_arr kw
val minute    : int32_arr kw
val second    : int32_arr kw
val time      : int32_arr kw
val datetime  : float64_arr kw

type vector
(** Type of a vector. *)

val bool_vect      : bool_arr     -> vector
val guid_vect      : uint8_arr    -> vector
val byte_vect      : uint8_arr    -> vector
val short_vect     : int16_arr    -> vector
val int_vect       : int32_arr    -> vector
val long_vect      : int64_arr    -> vector
val real_vect      : float32_arr  -> vector
val float_vect     : float64_arr  -> vector
val char_vect      : char_arr     -> vector
val symbol_vect    : string array -> vector
val timestamp_vect : int64_arr    -> vector
val month_vect     : int32_arr    -> vector
val date_vect      : int32_arr    -> vector
val timespan_vect  : int64_arr    -> vector
val minute_vect    : int32_arr    -> vector
val second_vect    : int32_arr    -> vector
val time_vect      : int32_arr    -> vector
val datetime_vect  : float64_arr  -> vector
(** Vector constructors *)

val get_vector : 'a kw -> vector -> 'a option
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
  | Timestamp of Ptime.t
  | Month     of int
  | Date      of int
  | Timespan  of Ptime.time * int
  | Minute    of int * int
  | Second    of int * int * int
  | Time      of Ptime.time * int
  | Datetime  of float

val pack : t -> k
(** [pack t] packs [t] into a K object. *)

val unpack : k -> t
(** [unpack k] is the OCaml representation of [k], a K object. *)

val bigstring_of_k : k -> char_arr option
(** [bigstring_of_k k] is [Some arr] iff [k] is a char vector. *)

val of_bigstring : char_arr -> (k, string) result
val of_bigstring_exn : char_arr -> k

val serialize : ?mode:int -> k -> (char_arr, string) result

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
