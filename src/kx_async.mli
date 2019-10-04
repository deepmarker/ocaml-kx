open Async
open Kx

type msg
val create : ?big_endian:bool -> 'a w -> 'a -> msg

module Async : sig
  type t =  {
    af: 'a. 'a w -> 'a Deferred.Or_error.t;
    w: msg Pipe.Writer.t
  }

  val empty : t

  val connect :
    ?comp:bool -> ?buf:Faraday.t ->
    Uri.t -> t Deferred.Or_error.t

  val with_connection :
    ?comp:bool -> ?buf:Faraday.t ->
    Uri.t -> f:(t -> 'b Deferred.t) ->
    'b Deferred.Or_error.t

  module Persistent : Persistent_connection_kernel.S
    with type conn := t
     and type address := Uri.t
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
