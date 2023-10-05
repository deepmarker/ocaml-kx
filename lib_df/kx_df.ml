open Kx
open Dataframe

let conv_df f a = conv (fun _ -> assert false) f a

let table1 ?sorted ?attr names k1 v1 =
  table
    ?sorted
    (conv_df
       (fun a ->
         let packed = Column.(P (of_array k1 a)) in
         Df.create_exn [ names.(0), packed ])
       (t1 (v ?attr v1)))
;;

let table2 ?sorted names k1 k2 v1 v2 =
  let attr =
    match sorted with
    | Some true -> Some Parted
    | _ -> None
  in
  table
    ?sorted
    (conv_df
       (fun (a, b) ->
         let p1 = Column.(P (of_array k1 a)) in
         let p2 = Column.(P (of_array k2 b)) in
         Df.create_exn [ names.(0), p1; names.(1), p2 ])
       (t2 (v ?attr v1) (v v2)))
;;

let table3 ?sorted names k1 k2 k3 v1 v2 v3 =
  let attr =
    match sorted with
    | Some true -> Some Parted
    | _ -> None
  in
  table
    ?sorted
    (conv_df
       (fun (a, b, c) ->
         let p1 = Column.(P (of_array k1 a)) in
         let p2 = Column.(P (of_array k2 b)) in
         let p3 = Column.(P (of_array k3 c)) in
         Df.create_exn [ names.(0), p1; names.(1), p2; names.(2), p3 ])
       (t3 (v ?attr v1) (v v2) (v v3)))
;;

let table4 ?sorted names k1 k2 k3 k4 v1 v2 v3 v4 =
  let attr =
    match sorted with
    | Some true -> Some Parted
    | _ -> None
  in
  table
    ?sorted
    (conv_df
       (fun (a, b, c, d) ->
         let p1 = Column.(P (of_array k1 a)) in
         let p2 = Column.(P (of_array k2 b)) in
         let p3 = Column.(P (of_array k3 c)) in
         let p4 = Column.(P (of_array k4 d)) in
         Df.create_exn [ names.(0), p1; names.(1), p2; names.(2), p3; names.(3), p4 ])
       (t4 (v ?attr v1) (v v2) (v v3) (v v4)))
;;

let table5 ?sorted names k1 k2 k3 k4 k5 v1 v2 v3 v4 v5 =
  let attr =
    match sorted with
    | Some true -> Some Parted
    | _ -> None
  in
  table
    ?sorted
    (conv_df
       (fun (a, b, c, d, e) ->
         let p1 = Column.(P (of_array k1 a)) in
         let p2 = Column.(P (of_array k2 b)) in
         let p3 = Column.(P (of_array k3 c)) in
         let p4 = Column.(P (of_array k4 d)) in
         let p5 = Column.(P (of_array k5 e)) in
         Df.create_exn
           [ names.(0), p1; names.(1), p2; names.(2), p3; names.(3), p4; names.(4), p5 ])
       (t5 (v ?attr v1) (v v2) (v v3) (v v4) (v v5)))
;;
