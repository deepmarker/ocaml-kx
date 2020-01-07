open Core
open Async
open Kx

type msg
val create : ?big_endian:bool -> 'a w -> 'a -> msg
val pp_serialized : Format.formatter -> msg -> unit

type t =  {
  r: 'a. 'a w option -> 'a Deferred.t;
  w: msg Pipe.Writer.t
}

val empty : t
val process : Uri.t ->  Reader.t -> Writer.t -> t Deferred.t

val with_connection :
  ?version:Async_ssl.Version.t ->
  ?options:Async_ssl.Opt.t list ->
  ?buffer_age_limit:[ `At_most of Time.Span.t | `Unlimited ] ->
  ?interrupt:unit Deferred.t ->
  ?reader_buffer_size:int ->
  ?writer_buffer_size:int ->
  ?timeout:Time.Span.t ->
  Uri.t ->
  (('a w sexp_option -> 'a Deferred.t) ->
   msg Pipe.Writer.t -> 'b Deferred.t) ->
  'b Deferred.t

module Persistent : sig
  include Persistent_connection_kernel.S
    with type conn = t
     and type address = Uri.t

  val with_current_connection :
    t -> f:(conn -> 'a Deferred.t) -> 'a Deferred.t

  val create' :
    server_name:string ->
    ?on_event:(Event.t -> unit Deferred.t) ->
    ?retry_delay:(unit -> Core_kernel.Time_ns.Span.t) ->
    (unit -> Uri.t Deferred.Or_error.t) -> t
end
