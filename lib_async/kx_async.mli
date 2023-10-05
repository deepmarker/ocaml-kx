open Core
open Async

type t =
  { r : 'a. 'a Kx.w -> 'a Deferred.Or_error.t
  ; w : Kx.t Pipe.Writer.t
  }

val empty : t
val write : Writer.t -> Kx.t -> unit Deferred.t
val process : ?monitor:Monitor.t -> Uri.t -> Reader.t -> Writer.t -> t Deferred.t

module Persistent : sig
  include Persistent_connection_kernel.S with type conn = t

  val create'
    :  ?monitor:Monitor.t
    -> server_name:string
    -> ?on_event:(Uri.t Event.t -> unit Deferred.t)
    -> ?retry_delay:(unit -> Time_ns.Span.t)
    -> ?random_state:[ `Non_random | `State of Core.Random.State.t ]
    -> ?time_source:[ `Read ] Time_source.T1.t
    -> address:(module Persistent_connection_kernel.Address with type t = Uri.t)
    -> (unit -> (Uri.t, Error.t) result Deferred.t)
    -> t
end
