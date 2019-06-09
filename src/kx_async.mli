open Async_kernel
open Kx

type t
val create : 'a w -> 'a -> t

val connect :
  ?buf:Faraday.t -> Uri.t -> (t Pipe.Writer.t, string) result Deferred.t

val with_connection :
  ?buf:Faraday.t -> Uri.t -> f:(t Pipe.Writer.t -> 'a Deferred.t) ->
  ('a, string) result Deferred.t
