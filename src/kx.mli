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

val k_g : k -> int
val k_h : k -> int
val k_i : k -> int32
val k_j : k -> int64
val k_e : k -> float
val k_f : k -> float
val k_s : k -> string

val dj : int -> int
val ymd : int -> int -> int -> int

type ('a, 'b) storage = ('a, 'b, Bigarray.c_layout) Bigarray.Array1.t

type uint8_arr   = (int, Bigarray.int8_unsigned_elt) storage
type int16_arr   = (int, Bigarray.int16_signed_elt) storage
type int32_arr   = (int32, Bigarray.int32_elt) storage
type int64_arr   = (int64, Bigarray.int64_elt) storage
type float32_arr = (float, Bigarray.float32_elt) storage
type float64_arr = (float, Bigarray.float64_elt) storage

(* val bool_arr : bool array -> uint8_arr
 * val uint8_arr : int array -> uint8_arr
 * val int16_arr : int array -> int16_arr
 * val int32_arr : int32 array -> int32_arr
 * val int64_arr : int64 array -> int64_arr
 * val float32_arr : float array -> float32_arr
 * val float64_arr : float array -> float64_arr *)

(* val timestamp_arr : Ptime.t array -> int64_arr *)
(* val guid_arr : Uuidm.t list -> Bigstring.t *)
val guids_of_arr : Bigstring.t -> Uuidm.t list

type _ kw
(** Type witness for k types. *)

(** Values for k-types type witnesses *)

val bool      : uint8_arr kw
val guid      : Bigstring.t kw
val byte      : uint8_arr kw
val short     : int16_arr kw
val int       : int32_arr kw
val long      : int64_arr kw
val real      : float32_arr kw
val float     : float64_arr kw
val char      : Bigstring.t kw
val symbol    : string list kw
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

type time = { time: Ptime.time ; ms: int }
type timespan = { time: Ptime.time ; ns: int }

(** Vector constructors *)

module Vect     : sig
  val bool      : uint8_arr    -> vector
  val guid      : Bigstring.t  -> vector
  val byte      : uint8_arr    -> vector
  val short     : int16_arr    -> vector
  val int       : int32_arr    -> vector
  val long      : int64_arr    -> vector
  val real      : float32_arr  -> vector
  val float     : float64_arr  -> vector
  val char      : Bigstring.t  -> vector
  val symbol    : string list  -> vector
  val timestamp : int64_arr    -> vector
  val month     : int32_arr    -> vector
  val date      : int32_arr    -> vector
  val time      : int32_arr    -> vector
  val timespan  : int64_arr    -> vector
  val minute    : int32_arr    -> vector
  val second    : int32_arr    -> vector
  val datetime  : float64_arr  -> vector
end

val get_vector : 'a kw -> vector -> 'a option
(** [get_vector typ v] is the underlying storage of the vector
    value. *)

type t =
  | Atom      of atom
  | Vector    of vector
  | List      of t list
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
  | Date      of Ptime.date
  | Time      of time
  | Timespan  of timespan
  | Minute    of Ptime.time
  | Second    of Ptime.time
  | Datetime  of float

module Atom     : sig
  val bool      : bool       -> t
  val guid      : Uuidm.t    -> t
  val byte      : int        -> t
  val short     : int        -> t
  val int       : int32      -> t
  val long      : int64      -> t
  val real      : float      -> t
  val float     : float      -> t
  val char      : char       -> t
  val symbol    : string     -> t
  val timestamp : Ptime.t    -> t
  val month     : int        -> t
  val date      : Ptime.date -> t
  val minute    : Ptime.time -> t
  val second    : Ptime.time -> t
  val time      : time       -> t
  val timespan  : timespan   -> t
end

module VectArray : sig
  val bool      : bool array       -> t
  val guid      : Uuidm.t array    -> t
  val byte      : int array        -> t
  val short     : int array        -> t
  val int       : int32 array      -> t
  val long      : int64 array      -> t
  val real      : float array      -> t
  val float     : float array      -> t
  val char      : string           -> t
  val symbol    : string array     -> t
  val timestamp : Ptime.t array    -> t
  val month     : int array        -> t
  val date      : Ptime.date array -> t
  val time      : time array       -> t
  val timespan  : timespan array   -> t
  val minute    : Ptime.time array -> t
  val second    : Ptime.time array -> t
  val datetime  : float array      -> t
end

val equal : t -> t -> bool
val pp : Format.formatter -> t -> unit

val zero_timespan : atom
val create_timespan :
  ?tz_offset:int -> hh:int -> mm:int ->
  ss:int -> ns:int -> unit -> atom

val atom : atom -> t
val vector : vector -> t
val list : t list -> t
val dict : t -> t -> t
val table : t -> t -> t

val ktrue : k
val kfalse : k

val pack : t -> k
(** [pack t] packs [t] into a K object. *)

val unpack : k -> t
(** [unpack k] is the OCaml representation of [k], a K object. *)

val of_bigstring : Bigstring.t -> (k, string) result
val of_bigstring_exn : Bigstring.t -> k
val to_bigstring : ?mode:int -> k -> Bigstring.t

(**/*)

type connection_error =
  | Authentication
  | Connection
  | Timeout
  | OpenSSL

val pp_connection_error :
  Format.formatter -> connection_error -> unit

type capability =
  | OneTBLimit
  | UseTLS

val connect :
  ?timeout:Ptime.span ->
  ?capability:capability -> Uri.t ->
  (Unix.file_descr, connection_error) result

val with_connection :
  ?timeout:Ptime.span ->
  ?capability:capability ->
  Uri.t -> f:(Unix.file_descr -> 'a) ->
  ('a, connection_error) result

val kclose : Unix.file_descr -> unit

val kread : Unix.file_descr -> k

val k0 : Unix.file_descr -> string -> unit
val k1 : Unix.file_descr -> string -> k -> unit
val k2 : Unix.file_descr -> string -> k -> k -> unit
val k3 : Unix.file_descr -> string -> k -> k -> k -> unit
val kn : Unix.file_descr -> string -> k array -> unit

val k0_sync : Unix.file_descr -> string -> k
val k1_sync : Unix.file_descr -> string -> k -> k
val k2_sync : Unix.file_descr -> string -> k -> k -> k
val k3_sync : Unix.file_descr -> string -> k -> k -> k -> k
val kn_sync : Unix.file_descr -> string -> k array -> k

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
