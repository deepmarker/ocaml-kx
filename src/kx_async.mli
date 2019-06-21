open Async_kernel
open Kx

type t
val create : 'a w -> 'a -> t

type error = [
  | `ConnectionTerminated
  | `Exn of exn
  | `ProtoError of int
  | `Eof
] [@@deriving sexp]

val pp_print_error : Format.formatter -> error -> unit

val connect :
  ?buf:Faraday.t -> Uri.t ->
  (t Pipe.Writer.t, error) result Deferred.t

val with_connection :
  ?buf:Faraday.t -> Uri.t -> f:(t Pipe.Writer.t -> 'a Deferred.t) ->
  ('a, error) result Deferred.t
