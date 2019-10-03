open Async
open Kx

type t

val create :
  ?big_endian:bool ->
  ?typ:[`Sync | `Async] ->
  'a w -> 'a -> t

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

type connection_async = {
  af: 'a. 'a w -> ('a, [`Q of string | `Angstrom of string]) result Deferred.t;
  w: t Pipe.Writer.t
}

val closed_connection_async : connection_async

val connect_async :
  ?comp:bool ->
  ?buf:Faraday.t ->
  Uri.t -> (connection_async, error) result Deferred.t

val with_connection_async :
  ?comp:bool ->
  ?buf:Faraday.t ->
  Uri.t -> f:(connection_async -> 'b Deferred.t) ->
  ('b, error) result Deferred.t

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
