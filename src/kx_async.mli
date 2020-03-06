open Core
open Async
open Kx

type msg

val create : ?big_endian:bool -> 'a w -> 'a -> msg

val pp_serialized : Format.formatter -> msg -> unit

type t = { r : 'a. 'a w option -> 'a Deferred.t; w : msg Pipe.Writer.t }

val empty : t

val with_connection :
  url:Uri.t ->
  f:(t -> 'b Deferred.t) ->
  ?version:Async_ssl.Version.t ->
  ?options:Async_ssl.Opt.t list ->
  (unit -> 'b Deferred.t) Async.Tcp.with_connect_options

module Persistent : sig
  include
    Persistent_connection_kernel.S with type conn = t and type address = Uri.t

  val with_current_connection : t -> f:(conn -> 'a Deferred.t) -> 'a Deferred.t

  val create' :
    ?monitor:Monitor.t ->
    server_name:string ->
    ?on_event:(Event.t -> unit Deferred.t) ->
    ?retry_delay:(unit -> Time_ns.Span.t) ->
    ?random_state:Random.State.t ->
    ?time_source:Time_source.t ->
    (unit -> address Or_error.t Deferred.t) ->
    t
end
