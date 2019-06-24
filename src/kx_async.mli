open Async_kernel
open Kx

type t
val create : 'a w -> 'a -> t

type error = [
  | `ConnectionTerminated
  | `Exn of exn
  | `ProtoError of int
  | `Eof
]

val pp_print_error : Format.formatter -> error -> unit

val connect :
  ?buf:Faraday.t -> Uri.t ->
  (('a w -> (hdr * 'a, string) result Deferred.t) * t Pipe.Writer.t, error) result Deferred.t

val with_connection :
  ?buf:Faraday.t -> Uri.t ->
  f:(('a w -> (hdr * 'a, string) result Deferred.t) -> t Pipe.Writer.t -> 'a Deferred.t) ->
  ('a, error) result Deferred.t
