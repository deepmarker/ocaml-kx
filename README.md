# ocaml-kx [![Build Status](https://travis-ci.com/deepmarker/ocaml-kx.svg?branch=master)](https://travis-ci.com/github/deepmarker/ocaml-kx)

## Introduction

This library interfaces OCaml to [kdb+/q](https://code.kx.com/q) from
[Kx Systems](https://kx.com), the world’s fastest time-series
database.

Kdb+ mostly is an interpreter for the
[q](https://code.kx.com/q/learn/startingkdb/language) language, with
an associated runtime able to seamlessly store data in memory or on
disk.

The interface is done through Kdb+
[IPC](https://code.kx.com/v2/basics/ipc) protocol. OCaml <-> Kdb+ type
conversion is done through *encodings*, which are OCaml values
representing q's datatypes.

Functions are then provided to, through so-called encoders and
decoders, convert OCaml values to corresponding q values, enabling
exchanging data between Kdb+ and an OCaml program.

## Dependencies

* The parsing and printing of q IPC is done with
  [Angstrom](https://github.com/inhabitedtype/angstrom) and
    [Faraday](https://github.com/inhabitedtype/faraday).

* Jane Street's [Async](https://opensource.janestreet.com/async) is
  used (in a separate module) to handle connectivity to a Kdb+ database.

