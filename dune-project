(lang dune 3.10)

(name kx)

(generate_opam_files true)

(source
 (github vbmithr/logs-async))

(authors "Vincent Bernardoff <vb@luminar.eu.org>")

(maintainers "Vincent Bernardoff <vb@luminar.eu.org>")

(license ISC)

(documentation https://deepmarker.github.io/ocaml-kx)

(package
 (name kx)
 (synopsis "OCaml client for KDB+")
 (description "")
 (depends
   ocaml
   dune
  (sexplib (>= v0.16))
  (ppx_sexp_conv (>= v0.16))
  (uuidm (>= 0.9.8))
  (ptime (>= 1.1.0))
  (ocplib-endian (>= 1.2))
  (faraday (>= 0.8.2))
  (angstrom (>= 0.15))
 )
 (tags (kdb+))
)

(package
 (name kx-async)
 (synopsis "OCaml client for KDB+ - Async code")
 (description "")
 (depends
   ocaml
   dune
  (kx (= :version))
  (core (>= v0.16))
  (async (>= v0.16))
  (uri-sexp (>= 4.4.0))
  (async-uri (>= 0.3.0))
  (logs-async (>= 1.1))
  (faraday-async (>= 0.8.2))
  (angstrom-async (>= 0.15.0))
 )
 (tags
  (kdb+ async)))

; See the complete stanza docs at https://dune.readthedocs.io/en/stable/dune-files.html#dune-project
