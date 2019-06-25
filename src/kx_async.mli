open Async
open Kx

type t

val create :
  ?endianness:[`Big | `Little] ->
  ?typ:[`Sync | `Async] ->
  'a w -> 'a -> t

type error = [
  | `Angstrom of string
  | `ConnectionTerminated
  | `Exn of exn
  | `ProtoError of int
  | `Eof
]

val pp_print_error : Format.formatter -> error -> unit

val connect_async :
  ?buf:Faraday.t -> Uri.t ->
  (('a w -> (hdr * 'a, string) result Deferred.t) * t Pipe.Writer.t, error) result Deferred.t

val with_connection_async :
  ?buf:Faraday.t -> Uri.t ->
  f:(('a w -> (hdr * 'a, string) result Deferred.t) -> t Pipe.Writer.t -> 'a Deferred.t) ->
  ('a, error) result Deferred.t

type f = {
  f: 'a 'b. ('a w -> 'a -> 'b w -> (hdr * 'b, error) result Deferred.t)
}

val connect_sync :
  ?endianness:[`Big | `Little] ->
  ?buf:Faraday.t -> Uri.t ->
  (f * Reader.t * Writer.t, error) result Deferred.t

val with_connection_sync :
  ?endianness:[`Big | `Little] ->
  ?buf:Faraday.t -> Uri.t ->
  f:(f -> 'c Deferred.t) -> ('c, error) result Deferred.t
