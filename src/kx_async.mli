open Async_kernel
open Kx

(** Connect to kdb+ *)

val connect :
  ?credentials:string * string ->
  ?timeout:int ->
  ?capability:capability ->
  host:string -> port:int -> unit ->
  (k Pipe.Reader.t * (string * k array) Pipe.Writer.t,
   connection_error) result

val with_connection :
  ?credentials:string * string ->
  ?timeout:int ->
  ?capability:capability ->
  host:string -> port:int ->
  (k Pipe.Reader.t -> (string * k array) Pipe.Writer.t -> 'a Deferred.t) ->
  ('a, connection_error) result Deferred.t
