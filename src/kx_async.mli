open Async_kernel
open Kx_final

val connect :
  ?timeout:Ptime.span ->
  ?capability:capability ->
  Uri.t ->
  (('a w -> 'a option) Pipe.Reader.t * (string * t array) Pipe.Writer.t,
   connection_error) result

val with_connection :
  ?timeout:Ptime.span ->
  ?capability:capability ->
  Uri.t ->
  f:(('a w -> 'a option) Pipe.Reader.t ->
     (string * t array) Pipe.Writer.t -> 'a Deferred.t) ->
  ('a, connection_error) result Deferred.t
