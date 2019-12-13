open Async
open Kx

type msg
val create : ?big_endian:bool -> 'a w -> 'a -> msg
val pp_serialized : Format.formatter -> msg -> unit

type t =  {
  r: 'a. 'a w option -> 'a Deferred.Or_error.t;
  w: msg Pipe.Writer.t
}

val empty : t

val process : t Ivar.t -> Uri.t ->  Reader.t -> Writer.t -> unit Deferred.t

val connect : Uri.t -> t Deferred.Or_error.t
val with_connection : Uri.t -> f:(t -> 'b Deferred.Or_error.t) -> 'b Deferred.Or_error.t

module Persistent : sig
  include Persistent_connection_kernel.S
    with type conn = t
     and type address = Uri.t

  val with_current_connection :
    t -> f:(conn -> 'a Deferred.Or_error.t) -> 'a Deferred.Or_error.t

  val create' :
    server_name:string ->
    ?on_event:(Event.t -> unit Deferred.t) ->
    ?retry_delay:(unit -> Core_kernel.Time_ns.Span.t) ->
    (unit -> Uri.t Deferred.Or_error.t) -> t
end
