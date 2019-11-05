open Async
open Kx

type msg
val create : ?big_endian:bool -> 'a w -> 'a -> msg
val pp_serialized : Format.formatter -> msg -> unit

module Async : sig
  type t =  {
    r: 'a. 'a w -> 'a Deferred.Or_error.t;
    w: msg Pipe.Writer.t
  }

  val empty : t

  val connect :
    ?comp:bool -> ?buf:Faraday.t ->
    Uri.t -> t Deferred.Or_error.t

  val with_connection :
    ?comp:bool -> ?buf:Faraday.t ->
    Uri.t -> f:(t -> 'b Deferred.Or_error.t) ->
    'b Deferred.Or_error.t

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
      ?comp:bool ->
      ?buf:Faraday.t -> (unit -> Uri.t Deferred.Or_error.t) -> t
  end
end

type sf = {
  sf: 'a 'b. ('a w -> 'a -> 'b w -> 'b Deferred.Or_error.t)
}

val connect_sync :
  ?comp:bool ->
  ?big_endian:bool ->
  ?buf:Faraday.t -> Uri.t ->
  (sf * Reader.t * Writer.t) Deferred.Or_error.t

val with_connection_sync :
  ?comp:bool ->
  ?big_endian:bool ->
  ?buf:Faraday.t -> Uri.t ->
  f:(sf -> 'a Deferred.t) -> 'a Deferred.Or_error.t
