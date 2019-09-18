(*---------------------------------------------------------------------------
   Copyright (c) 2018 Vincent Bernardoff. All rights reserved.
   Distributed under the ISC license, see terms at the end of the file.
  ---------------------------------------------------------------------------*)

type time = { time : Ptime.time ; ms : int }
type timespan = { time : Ptime.time ; ns : int }

type attribute =
  | Sorted
  | Unique
  | Parted
  | Grouped

type _ typ
type _ w
type _ case

val nil       : unit typ
val bool      : bool typ
val guid      : Uuidm.t typ
val byte      : char typ
val short     : int typ
val int       : int32 typ
val long      : int64 typ
val real      : float typ
val float     : float typ
val char      : char typ
val sym       : string typ
val timestamp : Ptime.t typ
val month     : Ptime.date typ
val date      : Ptime.date typ
val timespan  : timespan typ
val minute    : Ptime.time typ
val second    : Ptime.time typ
val time      : time typ
val lambda    : (string * string) typ

val err : string w
val a : 'a typ -> 'a w
val v : ?attr:attribute -> 'a typ -> 'a list w
val s : ?attr:attribute -> char typ -> string w

val list : ?attr:attribute -> 'a w -> 'a list w
val t1 : ?attr:attribute -> 'a w -> 'a w
val t2 : ?attr:attribute -> 'a w -> 'b w -> ('a * 'b) w
val t3 : ?attr:attribute -> 'a w -> 'b w -> 'c w -> ('a * 'b * 'c) w
val t4 : ?attr:attribute -> 'a w -> 'b w -> 'c w -> 'd w -> ('a * 'b * 'c * 'd) w
val t5 : ?attr:attribute -> 'a w -> 'b w -> 'c w -> 'd w -> 'e w -> ('a * 'b * 'c * 'd * 'e) w
val t6 : ?attr:attribute -> 'a w -> 'b w -> 'c w -> 'd w -> 'e w -> 'f w -> ('a * 'b * 'c * 'd * 'e * 'f) w
val t7 : ?attr:attribute -> 'a w -> 'b w -> 'c w -> 'd w -> 'e w -> 'f w -> 'g w -> ('a * 'b * 'c * 'd * 'e * 'f * 'g) w
val t8 : ?attr:attribute -> 'a w -> 'b w -> 'c w -> 'd w -> 'e w -> 'f w -> 'g w -> 'h w -> ('a * 'b * 'c * 'd * 'e * 'f * 'g * 'h) w
val t9 : ?attr:attribute -> 'a w -> 'b w -> 'c w -> 'd w -> 'e w -> 'f w -> 'g w -> 'h w -> 'i w -> ('a * 'b * 'c * 'd * 'e * 'f * 'g * 'h * 'i) w
val t10 : ?attr:attribute -> 'a w -> 'b w -> 'c w -> 'd w -> 'e w -> 'f w -> 'g w -> 'h w -> 'i w -> 'j w -> ('a * 'b * 'c * 'd * 'e * 'f * 'g * 'h * 'i * 'j) w

val merge_tups : 'a w -> 'b w -> ('a * 'b) w

val dict : ?sorted:bool -> 'a w -> 'b w -> ('a * 'b) w
val table : ?sorted:bool -> 'a w -> 'b w -> ('a * 'b) w

val conv : ('a -> 'b) -> ('b -> 'a) -> 'b w -> 'a w
val case : 'a w -> ('b -> 'a option) -> ('a -> 'b) -> 'b case
val union : 'a case list -> 'a w

type hdr = {
  big_endian: bool ;
  typ: [`Async | `Sync | `Response] ;
  compressed: bool ;
  len: int32 ;
} [@@deriving sexp]

val pp_print_hdr : Format.formatter -> hdr -> unit
val hdr : hdr Angstrom.t
(* val write_hdr : Faraday.t -> hdr -> unit *)

val construct :
  ?comp:bool ->
  ?big_endian:bool ->
  typ:[< `Async | `Sync | `Response] ->
  ?buf:Faraday.t -> 'a w -> 'a -> Bigstringaf.t

val destruct :
  ?big_endian:bool -> 'a w -> ('a, string) result Angstrom.t
val destruct_exn :
  ?big_endian:bool -> 'a w -> 'a Angstrom.t
val destruct_stream :
  ?big_endian:bool ->
  'a list w -> ('a -> unit) -> unit Angstrom.t

val nh : int
val wh : int

val ni : int32
val wi : int32

val nj : int64
val wj : int64

val nf : float
val wf : float
val ptime_neginf : Ptime.t

(** Timespan *)
val nn : timespan
val wn : timespan
val minus_wn : timespan

(** Time *)
val nt : time
val wt : time
val minus_wt : time

(** Month *)
val nm : Ptime.date
val wm : Ptime.date
val minus_wm : Ptime.date

(** Date *)
val nd : Ptime.date
val wd : Ptime.date
val minus_wd : Ptime.date

(** Minute *)
val nu : Ptime.time
val wu : Ptime.time
val minus_wu : Ptime.time

(** Second *)
val nv : Ptime.time
val wv : Ptime.time
val minus_wv : Ptime.time

val equal_w : 'a w -> 'b w -> bool
val equal : 'a w -> 'a -> 'b w -> 'b -> bool
val pp : 'a w -> Format.formatter -> 'a -> unit

(**/*)

val int32_of_month : Ptime.date -> int32
val month_of_int32 : int32 -> Ptime.date

val int32_of_date : Ptime.date -> int32
val date_of_int32 : int32 -> Ptime.date

val int32_of_time : time -> int32
val time_of_int32 : int32 -> time

val pp_print_date : (int * int * int) Fmt.t

val compress   : ?big_endian:bool -> Bigstringaf.t -> Bigstringaf.t
val uncompress : Bigstringaf.t -> Bigstringaf.t -> unit

module type ENDIAN = module type of Angstrom.BE
val getmod : bool -> (module ENDIAN)

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
