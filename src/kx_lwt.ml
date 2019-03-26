(*---------------------------------------------------------------------------
   Copyright (c) 2018 Vincent Bernardoff. All rights reserved.
   Distributed under the ISC license, see terms at the end of the file.
  ---------------------------------------------------------------------------*)

open Kx
type conn = Lwt_unix.file_descr

let connect ?timeout_ms ~host ~port ~username () =
  let res =
    match timeout_ms with
    | None -> khpu ~host ~port ~username
    | Some timeout_ms -> khpun ~host ~port ~username ~timeout_ms in
  match res with
  | Error string -> Error string
  | Ok fd ->
    Ok (Lwt_unix.of_unix_file_descr ~blocking:false fd)

let write0 c msg =
  let fd = Lwt_unix.unix_file_descr c in
  k0 fd msg

(* let write1 c msg a =
 *   let fd = Lwt_unix.unix_file_descr c in
 *   k1 fd msg (Kx.pack a) *)

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
