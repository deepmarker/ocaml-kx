type time = { time : Ptime.time ; ms : int }
type timespan = { time : Ptime.time ; ns : int }

module W : sig
  type _ t

  val bool : bool t
  val guid : Uuidm.t t
  val byte : char t
  val short : int t
  val int : int32 t
  val long : int64 t
  val real : float t
  val float : float t
  val char : char t
  val sym : string t
  val timestamp : Ptime.t t
  val month : Ptime.date t
  val date : Ptime.date t
  val timespan : timespan t
  val minute : Ptime.time t
  val second : Ptime.time t
  val time : time t

  val nil : unit t
  val cons : 'a t -> 'b t -> ('a * 'b) t
  val dict : 'a t -> 'b t -> ('a * 'b) t
  val table : 'a t -> 'b t -> ('a * 'b) t

  val conv : ('a -> 'b t) -> ('b t -> 'a) -> 'a -> 'b t
end

type k
type _ t

val construct : 'a W.t -> 'a -> 'a t
val destruct : 'a W.t -> k -> 'a option
