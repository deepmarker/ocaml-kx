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
]

val pp_print_error : Format.formatter -> error -> unit

val fail : error -> 'a
val fail_on_error : ('a, error) result -> 'a

type af = {
  af: 'a. 'a w -> (hdr * 'a, [`Q of string | `Angstrom of string]) result Deferred.t
}

val connect_async :
  ?buf:Faraday.t -> Uri.t -> (af * t Pipe.Writer.t, error) result Deferred.t

val with_connection_async :
  ?buf:Faraday.t -> Uri.t -> f:(af -> t Pipe.Writer.t -> 'b Deferred.t) ->
  ('b, error) result Deferred.t

type sf = {
  sf: 'a 'b. ('a w -> 'a -> 'b w -> (hdr * 'b, error) result Deferred.t)
}

val connect_sync :
  ?big_endian:bool ->
  ?buf:Faraday.t -> Uri.t ->
  (sf * Reader.t * Writer.t, error) result Deferred.t

val with_connection_sync :
  ?big_endian:bool ->
  ?buf:Faraday.t -> Uri.t ->
  f:(sf -> 'a Deferred.t) -> ('a, error) result Deferred.t
