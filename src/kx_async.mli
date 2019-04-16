open Async_kernel
open Kx

(** Connect to kdb+ *)

exception NetworkError
(** Raised when impossible to write to server *)

val connect :
  ?timeout:Ptime.span ->
  ?capability:capability ->
  Uri.t ->
  (k Pipe.Reader.t * (string * t array) Pipe.Writer.t,
   connection_error) result

val with_connection :
  ?timeout:Ptime.span ->
  ?capability:capability ->
  Uri.t ->
  f:(k Pipe.Reader.t -> (string * t array) Pipe.Writer.t -> 'a Deferred.t) ->
  ('a, connection_error) result Deferred.t
