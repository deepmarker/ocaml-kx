type time = { time : Ptime.time ; ms : int }
type timespan = { time : Ptime.time ; ns : int }

type k
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
val v : 'a typ -> 'a array w
val s : char typ -> string w

val list : k array w
val t1 : 'a w -> 'a w
val t2 : 'a w -> 'b w -> ('a * 'b) w
val t3 : 'a w -> 'b w -> 'c w -> ('a * 'b * 'c) w
val t4 : 'a w -> 'b w -> 'c w -> 'd w -> ('a * 'b * 'c * 'd) w
val t5 : 'a w -> 'b w -> 'c w -> 'd w -> 'e w -> ('a * 'b * 'c * 'd * 'e) w
val t6 : 'a w -> 'b w -> 'c w -> 'd w -> 'e w -> 'f w -> ('a * 'b * 'c * 'd * 'e * 'f) w
val t7 : 'a w -> 'b w -> 'c w -> 'd w -> 'e w -> 'f w -> 'g w -> ('a * 'b * 'c * 'd * 'e * 'f * 'g) w
val t8 : 'a w -> 'b w -> 'c w -> 'd w -> 'e w -> 'f w -> 'g w -> 'h w -> ('a * 'b * 'c * 'd * 'e * 'f * 'g * 'h) w
val t9 : 'a w -> 'b w -> 'c w -> 'd w -> 'e w -> 'f w -> 'g w -> 'h w -> 'i w -> ('a * 'b * 'c * 'd * 'e * 'f * 'g * 'h * 'i) w
val t10 : 'a w -> 'b w -> 'c w -> 'd w -> 'e w -> 'f w -> 'g w -> 'h w -> 'i w -> 'j w -> ('a * 'b * 'c * 'd * 'e * 'f * 'g * 'h * 'i * 'j) w

val merge_tups : 'a w -> 'b w -> ('a * 'b) w

val dict : 'a w -> 'b w -> ('a * 'b) w
val table : 'a w -> 'b w -> ('a * 'b) w

val conv : ('a -> 'b) -> ('b -> 'a) -> 'b w -> 'a w

type t

val pp : Format.formatter -> t -> unit

val equal_typ : t -> t -> bool
(** [equal_typ a b] is [true] iff [a] is the same q type as [b]. *)

val equal : t -> t -> bool
(** [equal_typ a b] is [true] iff [a] is the same q type and value as [b]. *)

val construct : 'a w -> 'a -> t
val destruct_k : 'a w -> k -> ('a, string) result
val destruct : 'a w -> t -> ('a, string) result

val of_string : 'a w -> string -> (t, string) result
val of_string_exn : 'a w -> string -> t
val to_string : ?mode:int -> t -> string

(** Connection to q server *)

val init : unit -> unit
(** [init ()] must be called before creating any q
    value. Initialization is also done automatically when connecting to
    a server. *)

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

val kread : Unix.file_descr -> 'a w -> ('a, string) result

val k0 : Unix.file_descr -> string -> unit
val k1 : Unix.file_descr -> string -> t -> unit
val k2 : Unix.file_descr -> string -> t -> t -> unit
val k3 : Unix.file_descr -> string -> t -> t -> t -> unit
val kn : Unix.file_descr -> string -> t array -> unit

val k0_sync : Unix.file_descr -> string -> 'a w -> ('a, string) result
val k1_sync : Unix.file_descr -> string -> 'a w -> t -> ('a, string) result
val k2_sync : Unix.file_descr -> string -> 'a w -> t -> t -> ('a, string) result
val k3_sync : Unix.file_descr -> string -> 'a w -> t -> t -> t -> ('a, string) result
val kn_sync : Unix.file_descr -> string -> 'a w -> t array -> ('a, string) result
