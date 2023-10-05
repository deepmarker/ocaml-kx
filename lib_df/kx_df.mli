val table1
  :  ?sorted:bool
  -> ?attr:attribute
  -> string array
  -> ('a, _) Array_intf.t
  -> 'a typ
  -> (string array * [ `unfiltered ] Df.t) w

val table2
  :  ?sorted:bool
  -> string array
  -> ('a, _) Array_intf.t
  -> ('b, _) Array_intf.t
  -> 'a typ
  -> 'b typ
  -> (string array * [ `unfiltered ] Df.t) w

val table3
  :  ?sorted:bool
  -> string array
  -> ('a, _) Array_intf.t
  -> ('b, _) Array_intf.t
  -> ('c, _) Array_intf.t
  -> 'a typ
  -> 'b typ
  -> 'c typ
  -> (string array * [ `unfiltered ] Df.t) w

val table4
  :  ?sorted:bool
  -> string array
  -> ('a, _) Array_intf.t
  -> ('b, _) Array_intf.t
  -> ('c, _) Array_intf.t
  -> ('d, _) Array_intf.t
  -> 'a typ
  -> 'b typ
  -> 'c typ
  -> 'd typ
  -> (string array * [ `unfiltered ] Df.t) w

val table5
  :  ?sorted:bool
  -> string array
  -> ('a, _) Array_intf.t
  -> ('b, _) Array_intf.t
  -> ('c, _) Array_intf.t
  -> ('d, _) Array_intf.t
  -> ('e, _) Array_intf.t
  -> 'a typ
  -> 'b typ
  -> 'c typ
  -> 'd typ
  -> 'e typ
  -> (string array * [ `unfiltered ] Df.t) w
