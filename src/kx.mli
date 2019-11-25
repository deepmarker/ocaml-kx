(*---------------------------------------------------------------------------
   Copyright (c) 2018 Vincent Bernardoff. All rights reserved.
   Distributed under the ISC license, see terms at the end of the file.
  ---------------------------------------------------------------------------*)

type attribute =
  | Sorted
  | Unique
  | Parted
  | Grouped

type _ typ
type _ w
type _ case

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
val timespan  : Ptime.Span.t typ
val minute    : Ptime.Span.t typ
val second    : Ptime.Span.t typ
val time      : Ptime.Span.t typ
val lambda    : (string * string) typ
val unaryprim : int typ
val operator  : int typ
val over      : int typ

module Unary : sig
  val id  : int
  val neg : int
  val sum : int
  val prd : int
end

module Op : sig
  val plus  : int
  val minus : int
  val prd   : int
  val div   : int
  val join  : int
end

val err : string w
val a : 'a typ -> 'a w
val v : ?attr:attribute -> 'a typ -> 'a array w
val s : ?attr:attribute -> char typ -> string w
val unit : unit w

val list : ?attr:attribute -> 'a w -> 'a array w
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

val cd1 : ?sorted:bool -> ?attr:attribute -> 'a typ -> (string array * 'a) w
val cd2 : ?sorted:bool -> ?attr:attribute -> 'a typ -> 'b typ -> (string array * ('a * 'b)) w
val cd3 : ?sorted:bool -> ?attr:attribute -> 'a typ -> 'b typ -> 'c typ -> (string array * ('a * 'b * 'c)) w
val cd4 : ?sorted:bool -> ?attr:attribute -> 'a typ -> 'b typ -> 'c typ -> 'd typ -> (string array * ('a * 'b * 'c * 'd)) w
val cd5 : ?sorted:bool -> ?attr:attribute -> 'a typ -> 'b typ -> 'c typ -> 'd typ -> 'e typ -> (string array * ('a * 'b * 'c * 'd * 'e)) w
val cd6 : ?sorted:bool -> ?attr:attribute -> 'a typ -> 'b typ -> 'c typ -> 'd typ -> 'e typ -> 'f typ -> (string array * ('a * 'b * 'c * 'd * 'e * 'f)) w
val cd7 : ?sorted:bool -> ?attr:attribute -> 'a typ -> 'b typ -> 'c typ -> 'd typ -> 'e typ -> 'f typ -> 'g typ -> (string array * ('a * 'b * 'c * 'd * 'e * 'f * 'g)) w
val cd8 : ?sorted:bool -> ?attr:attribute -> 'a typ -> 'b typ -> 'c typ -> 'd typ -> 'e typ -> 'f typ -> 'g typ -> 'h typ -> (string array * ('a * 'b * 'c * 'd * 'e * 'f * 'g * 'h)) w
val cd9 : ?sorted:bool -> ?attr:attribute -> 'a typ -> 'b typ -> 'c typ -> 'd typ -> 'e typ -> 'f typ -> 'g typ -> 'h typ -> 'i typ -> (string array * ('a * 'b * 'c * 'd * 'e * 'f * 'g * 'h * 'i)) w

val table : ?sorted:bool -> 'a w -> (string array * 'a) w

val table1: ?sorted:bool -> ?attr1:attribute -> ?attr2:attribute -> 'a typ -> (string array * 'a array) w
val table2: ?sorted:bool -> 'a typ -> 'b typ -> (string array * ('a array * 'b array)) w
val table3: ?sorted:bool -> 'a typ -> 'b typ -> 'c typ -> (string array * ('a array * 'b array * 'c array)) w
val table4: ?sorted:bool -> 'a typ -> 'b typ -> 'c typ -> 'd typ -> (string array * ('a array * 'b array * 'c array * 'd array)) w
val table5: ?sorted:bool -> 'a typ -> 'b typ -> 'c typ -> 'd typ -> 'e typ -> (string array * ('a array * 'b array * 'c array * 'd array * 'e array)) w
val table6: ?sorted:bool -> 'a typ -> 'b typ -> 'c typ -> 'd typ -> 'e typ -> 'f typ -> (string array * ('a array * 'b array * 'c array * 'd array * 'e array * 'f array)) w
val table7: ?sorted:bool -> 'a typ -> 'b typ -> 'c typ -> 'd typ -> 'e typ -> 'f typ -> 'g typ -> (string array * ('a array * 'b array * 'c array * 'd array * 'e array * 'f array * 'g array)) w
val table8: ?sorted:bool -> 'a typ -> 'b typ -> 'c typ -> 'd typ -> 'e typ -> 'f typ -> 'g typ -> 'h typ -> (string array * ('a array * 'b array * 'c array * 'd array * 'e array * 'f array * 'g array * 'h array)) w
val table9: ?sorted:bool -> 'a typ -> 'b typ -> 'c typ -> 'd typ -> 'e typ -> 'f typ -> 'g typ -> 'h typ -> 'i typ -> (string array * ('a array * 'b array * 'c array * 'd array * 'e array * 'f array * 'g array * 'h array * 'i array)) w

module Df : sig
  val table1: ?sorted:bool -> ?attr1:attribute -> ?attr2:attribute -> 'a typ -> (string array * 'a Dataframe.t) w
  val table2: ?sorted:bool -> 'a typ -> 'b typ -> (string array * ('a * 'b) Dataframe.t) w
  val table3: ?sorted:bool -> 'a typ -> 'b typ -> 'c typ -> (string array * ('a * 'b * 'c) Dataframe.t)  w
  val table4: ?sorted:bool -> 'a typ -> 'b typ -> 'c typ -> 'd typ -> (string array * ('a * 'b * 'c * 'd) Dataframe.t) w
  val table5: ?sorted:bool -> 'a typ -> 'b typ -> 'c typ -> 'd typ -> 'e typ -> (string array * ('a * 'b * 'c * 'd * 'e) Dataframe.t) w
  val table6: ?sorted:bool -> 'a typ -> 'b typ -> 'c typ -> 'd typ -> 'e typ -> 'f typ -> (string array * ('a * 'b * 'c * 'd * 'e * 'f) Dataframe.t) w
  val table7: ?sorted:bool -> 'a typ -> 'b typ -> 'c typ -> 'd typ -> 'e typ -> 'f typ -> 'g typ -> (string array * ('a * 'b * 'c * 'd * 'e * 'f * 'g) Dataframe.t) w
  val table8: ?sorted:bool -> 'a typ -> 'b typ -> 'c typ -> 'd typ -> 'e typ -> 'f typ -> 'g typ -> 'h typ -> (string array * ('a * 'b * 'c * 'd * 'e * 'f * 'g * 'h) Dataframe.t) w
  val table9: ?sorted:bool -> 'a typ -> 'b typ -> 'c typ -> 'd typ -> 'e typ -> 'f typ -> 'g typ -> 'h typ -> 'i typ -> (string array * ('a * 'b * 'c * 'd * 'e * 'f * 'g * 'h * 'i) Dataframe.t) w
end

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
  ?big_endian:bool -> 'a w -> 'a Angstrom.t
val destruct_stream :
  ?big_endian:bool ->
  'a array w -> ('a -> unit) -> unit Angstrom.t

val ng : Uuidm.t

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
val nn : Ptime.Span.t
val wn : Ptime.Span.t
val minus_wn : Ptime.Span.t

(** Timestamp *)
val np : Ptime.t
val wp : Ptime.t
val minus_wp : Ptime.t

(** Time *)
val nt : Ptime.Span.t
val wt : Ptime.Span.t
val minus_wt : Ptime.Span.t

(** Month *)
val nm : Ptime.date
val wm : Ptime.date
val minus_wm : Ptime.date

(** Date *)
val nd : Ptime.date
val wd : Ptime.date
val minus_wd : Ptime.date

(** Minute *)
val nu : Ptime.Span.t
val wu : Ptime.Span.t
val minus_wu : Ptime.Span.t

(** Second *)
val nv : Ptime.Span.t
val wv : Ptime.Span.t
val minus_wv : Ptime.Span.t

val equal_w : 'a w -> 'b w -> bool
val equal : 'a w -> 'a -> 'b w -> 'b -> bool
val pp : 'a w -> Format.formatter -> 'a -> unit

(**/*)

val int32_of_month : Ptime.date -> int32
val month_of_int32 : int32 -> Ptime.date

val int32_of_date : Ptime.date -> int32
val date_of_int32 : int32 -> Ptime.date

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
