(*---------------------------------------------------------------------------
   Copyright (c) 2018 Vincent Bernardoff. All rights reserved.
   Distributed under the ISC license, see terms at the end of the file.
  ---------------------------------------------------------------------------*)

type time = { time : Ptime.time ; ms : int }
type timespan = { time : Ptime.time ; ns : int }

type attribute =
  | NoAttr
  | Sorted
  | Unique
  | Parted
  | Grouped

type _ typ
type _ w

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

val a : 'a typ -> 'a w
val v : ?attr:attribute -> 'a typ -> 'a array w
val s : ?attr:attribute -> char typ -> string w

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
val table : ?sorted:bool -> 'a w -> 'b w -> ('a * 'b) w

val conv : ('a -> 'b) -> ('b -> 'a) -> 'b w -> 'a w

type hdr = {
  endianness: [`Little | `Big] ;
  typ: [`Async | `Sync | `Response] ;
  len: int32 ;
}

val string_of_hdr : hdr -> string
val construct :
  ?endianness:[`Big | `Little] ->
  ?typ:[`Async | `Sync | `Response] ->
  Faraday.t -> 'a w -> 'a -> hdr

val destruct :
  ?endianness:[`Big | `Little] -> 'a w -> (hdr * 'a) Angstrom.t

val nh : int
val ni : int32
val nj : int64
val nf : float

val wh : int
val wi : int32
val wj : int64
val wf : float

val equal_w : 'a w -> 'b w -> bool
val equal : 'a w -> 'a -> 'b w -> 'b -> bool
val pp : 'a w -> Format.formatter -> 'a -> unit

(**/*)

val int_of_month : Ptime.date -> int
val month_of_int : int -> Ptime.date

val int_of_date : Ptime.date -> int
val date_of_int : int -> Ptime.date

val pp_print_date : (int * int * int) Fmt.t

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
