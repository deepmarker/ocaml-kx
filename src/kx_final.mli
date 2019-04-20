type time = { time : Ptime.time ; ms : int }
type timespan = { time : Ptime.time ; ns : int }

module W : sig
  type _ typ
  type _ t

  val pp : Format.formatter -> _ t -> unit
  val equal : 'a t -> 'a t -> bool

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

  val atom : 'a typ -> 'a t
  val vect : 'a typ -> 'a array t
  val string : char typ -> string t
  val nil : unit t
  val cons : 'a t -> 'b t -> ('a * 'b) t
  val dict : 'a t -> 'b t -> ('a * 'b) t
  val table : 'a t -> 'b t -> ('a * 'b) t

  val conv : ('a -> 'b t) -> ('b t -> 'a) -> 'a -> 'b t
end

type k
type _ t

val pp : Format.formatter -> _ t -> unit
val equal : 'a t -> 'a t -> bool

val w : 'a t -> 'a W.t
val k : _ t -> k

val init : unit -> unit

val construct : 'a W.t -> 'a -> 'a t
val destruct : 'a W.t -> k -> 'a option
