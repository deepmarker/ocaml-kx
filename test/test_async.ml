open Core
open Async
open Kx
open Kx_async
open Alcotest_async

let test_headersless msg w () =
  let r =
    Pipe.create_reader ~close_on_exception:false (fun w -> Pipe.write w msg)
  in
  Reader.of_pipe (Info.of_string "") r >>= fun r ->
  Angstrom_async.parse Kx.(destruct w) r >>= function
  | Ok _v -> Deferred.unit
  | Error e -> failwith e

let test_async hex w () =
  let r =
    Pipe.create_reader ~close_on_exception:false (fun w ->
        Pipe.write w (Hex.to_string hex))
  in
  Reader.of_pipe (Info.of_string "") r >>= fun r ->
  Angstrom_async.parse Kx.hdr r >>= fun _hdr ->
  Angstrom_async.parse Kx.(destruct w) r >>= function
  | Ok _v -> Deferred.unit
  | Error e -> failwith e

let test_server () =
  let t =
    Kx_async.create
      (t3 (a Kx.operator) (a Kx.long) (a Kx.long))
      (Kx.Op.plus, 1L, 1L)
  in
  let url = Uri.make ~host:"localhost" ~port:5042 () in
  Kx_async.with_connection ~url
    ~f:(fun { w; _ } -> Pipe.write w t >>= fun () -> Deferred.Or_error.ok_unit)
    ()
  >>= function
  | Error e -> Error.raise e
  | Ok () -> Deferred.unit

let tests_kx_async =
  [
    test_case "vect symbol headerless" `Quick
      (test_headersless
         "\011\000\003\000\000\000countbysym\000hloc\000search\000"
         Kx.(v sym));
    test_case "atom symbol headerless" `Quick
      (test_headersless "\245a\000" Kx.(a sym));
    test_case "vect bool" `Quick
      (test_async (`Hex "0100000011000000010003000000010100") Kx.(v bool));
    test_case "vect char" `Quick
      (test_async (`Hex "01000000110000000a0003000000617569") Kx.(v char));
    test_case "vect byte" `Quick
      (test_async (`Hex "01000000100000000400020000001234") Kx.(v byte));
    test_case "vect short" `Quick
      (test_async (`Hex "0100000014000000050003000000010002000300")
         Kx.(v short));
    test_case "vect int" `Quick
      (test_async (`Hex "01000000160000000600020000000100000002000000")
         Kx.(v int));
    test_case "vect long" `Quick
      (test_async
         (`Hex "010000001e00000007000200000001000000000000000200000000000000")
         Kx.(v long));
    test_case "vect float" `Quick
      (test_async
         (`Hex "010000001e000000090002000000000000000000f03f0000000000000040")
         Kx.(v float));
    test_case "atom empty symbol" `Quick
      (test_async (`Hex "010000000a000000f500") Kx.(a sym));
    test_case "atom symbol" `Quick
      (test_async (`Hex "010000000b000000f56100") Kx.(a sym));
    test_case "vect symbol" `Quick
      (test_async (`Hex "01000000120000000b000200000061006200") Kx.(v sym));
    test_case "keyed_table" `Quick
      (test_async
         (`Hex
           "0100000068000000636200630b000100000073796d000000010000000b00010000005842545553442e434250006200630b000200000073697a65007472616465636f756e740000000200000009000100000064cc5d4bc8d788400700010000003b22000000000000")
         Kx.(dict (table1 sym) (table2 float long)));
    test_case "keyed_table2" `Quick
      (test_async
         (`Hex
           "0100000068000000636201630b000100000073796d000000010000000b00010000005842545553442e434250006200630b000200000073697a65007472616465636f756e740000000200000009000100000059f03b5dc8d788400700010000003b22000000000000")
         Kx.(dict (table1 ~sorted:true sym) (table2 float long)));
    test_case "trade0" `Quick
      (test_async
         (`Hex
           "01000000e70000006200630b000800000074696d650073796d00696400677569640070726963650073697a65007369646500666c6167000000080000000c0002000000c0e8f390dd60db08c0e8f390dd60db080b00020000005842545553442e424d58005842545553442e424d580007000200000000000000000000800000000000000080020002000000caf7bfe2ecd3b4dece05bc17955ebdf9284ee8cb7a7608af97774ebdbac9bae70900020000000000000000a7b7400000000000a7b740090002000000000000000000f03f00000000000022400a000200000073730a00020000007070")
         Kx.(table8 timestamp sym long guid float float char char));
    test_case "trade1" `Quick
      (test_async
         (`Hex
           "01000000240100006200630b000800000074696d650073796d00696400677569640070726963650073697a65007369646500666c6167000000080000000c000300000000645172de60db0800645172de60db0800645172de60db080b00030000005842545553442e424d58005842545553442e424d58005842545553442e424d580007000300000000000000000000800000000000000080000000000000008002000300000062f018c6fe2e41f1666567f3c8a0564d590a94a1f9bd54b1a77699565df2c9f8a64b4a4b714ab10d173e1258a2c51b470900030000000000000000a7b7400000000000a7b7400000000000a7b7400900030000000000000000faac4000000000002caf400000000000faa1400a00030000007373730a0003000000707070")
         Kx.(table8 timestamp sym long guid float float char char));
    test_case "trade2" `Quick
      (test_async
         (`Hex
           "01000000aa0000006200630b000800000074696d650073796d00696400677569640070726963650073697a65007369646500666c6167000000080000000c0001000000c0e55b99de60db080b00010000005842545553442e424d580007000100000000000000000000800200010000000002d1f376bf68fc6c19319056ae7b420900010000000000000000a7b7400900010000000000000000003f400a0001000000730a000100000070")
         Kx.(table8 timestamp sym long guid float float char char));
    test_case "trade3" `Quick
      (test_async
         (`Hex
           "01000000aa0000006200630b000800000074696d650073796d00696400677569640070726963650073697a65007369646500666c6167000000080000000c000100000080e397e5de60db080b00010000005842545553442e424d580007000100000000000000000000800200010000009cdd3336ac1ab33e9e9afe85b38c8cf80900010000000000000000a7b74009000100000000000000000036400a0001000000730a000100000070")
         Kx.(table8 timestamp sym long guid float float char char));
    (* test_case "server" `Quick test_server ; *)
  ]

let main () = run "kx_async" [ ("async", tests_kx_async) ]

let () =
  don't_wait_for (main ());
  never_returns (Scheduler.go ())
