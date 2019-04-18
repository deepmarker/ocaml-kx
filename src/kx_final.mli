type ('ml, 'c) bv =
  ('c, 'ml, Bigarray.c_layout) Bigarray.Array1.t

type 'a atom

type ('ml, 'c) vect

type 'a list
type ('a, 'b) dict
type ('a, 'b) table

val bool : bool -> bool atom

val bool_vect :
  (int, Bigarray.int8_unsigned_elt) bv ->
  (int, Bigarray.int8_unsigned_elt) vect

val list0 : unit list
val list1 : 'a -> 'a list
val list2 : 'a -> 'b -> ('a * 'b) list
val list3 : 'a -> 'b -> 'a -> ('a * 'b * 'c) list

val dict : 'a -> 'b -> ('a, 'b) dict
val table : 'a -> 'b -> ('a, 'b) table
