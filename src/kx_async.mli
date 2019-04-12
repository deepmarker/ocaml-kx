open Async_kernel
open Kx

(** Connect to kdb+ *)

val khpu :
  host:string -> port:int -> username:string ->
  (k Pipe.Reader.t * (string * k array) Pipe.Writer.t, string) result Deferred.t

val khpun :
  host:string -> port:int -> username:string -> timeout_ms:int ->
  (k Pipe.Reader.t * (string * k array) Pipe.Writer.t, string) result Deferred.t
