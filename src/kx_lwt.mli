(*---------------------------------------------------------------------------
   Copyright (c) 2018 Vincent Bernardoff. All rights reserved.
   Distributed under the ISC license, see terms at the end of the file.
  ---------------------------------------------------------------------------*)

type conn
(** Type of a connection to a kdb+ database. *)

val connect :
  ?timeout_ms:int ->
  host:string -> port:int -> username:string -> unit ->
  (conn, string) result
(** [connect ?timeout_ms ~host ~port ~username ()] is a connection to
    a kdb+ database at [host:port], identified by [username], with
    optional [timeout_ms] set. *)

val write0 : conn -> string -> (Kx.k, string) result
(** [w0 conn msg] asynchronously writes [msg] to [conn] and
    (immediately) returns either success or an error message. *)

val write1 : conn -> string -> Kx.t -> (Kx.k, string) result

(*---------------------------------------------------------------------------
   Copyright (c) 2018 Vincent Bernardoff

   Permission to use, copy, modify, and/or distribute this software for any
   purpose with or without fee is hereby granted, provided that the above
   copyright notice and this permission notice appear in all copies.

   THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
   WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
   MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
   ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
   WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
   ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
   OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
  ---------------------------------------------------------------------------*)
