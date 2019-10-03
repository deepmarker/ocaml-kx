open Core
open Async
open Kx

type msg

val create : ?big_endian:bool -> 'a w -> 'a -> msg

type error = [
  | `Q of string
  | `Angstrom of string
  | `ConnectionTerminated
  | `Exn of exn
  | `ProtoError of int
  | `Eof
] [@@deriving sexp_of]

val pp_print_error : Format.formatter -> error -> unit

val fail : error -> 'a
val fail_on_error : ('a, error) result -> 'a

module Async : sig
  type connection =  {
    af: 'a. 'a w -> ('a, [`Q of string | `Angstrom of string]) result Deferred.t;
    w: msg Pipe.Writer.t
  }

  include Persistent_connection_kernel.S with type conn := connection

  val empty : connection

  val connect :
    ?comp:bool -> ?buf:Faraday.t ->
    Uri.t -> (connection, error) Deferred.Result.t

  val with_connection :
    ?comp:bool -> ?buf:Faraday.t ->
    Uri.t -> f:(connection -> 'b Deferred.t) ->
    ('b, error) Deferred.Result.t
end

type sf = {
  sf: 'a 'b. ('a w -> 'a -> 'b w -> ('b, error) result Deferred.t)
}

val connect_sync :
  ?comp:bool ->
  ?big_endian:bool ->
  ?buf:Faraday.t -> Uri.t ->
  (sf * Reader.t * Writer.t, error) result Deferred.t

val with_connection_sync :
  ?comp:bool ->
  ?big_endian:bool ->
  ?buf:Faraday.t -> Uri.t ->
  f:(sf -> 'a Deferred.t) -> ('a, error) result Deferred.t
