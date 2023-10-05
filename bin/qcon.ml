let with_connection (x : Unix.addr_info) f =
  let ic, oc = Unix.open_connection x.ai_addr in
  try
    let ret = f ic oc in
    Unix.shutdown_connection ic;
    ret
  with
  | exn ->
    Unix.shutdown_connection ic;
    raise exn
;;

let onConn ?creds buf host port ic oc =
  let buflen = Bytes.length buf in
  let rec inputAll () =
    let nbRead = input ic buf 0 buflen in
    output stdout buf 0 nbRead;
    if nbRead = buflen then inputAll ()
  in
  print_string (host ^ ":" ^ port ^ ">");
  let req = read_line () in
  (match creds with
   | None -> output_string oc (req ^ "\x00")
   | Some (u, p) -> output_string oc (u ^ ":" ^ p ^ "\x00" ^ req ^ "\x00"));
  flush oc;
  inputAll ()
;;

let qcon ?creds host port =
  match Unix.getaddrinfo host port [ AI_FAMILY PF_INET; AI_SOCKTYPE SOCK_STREAM ] with
  | [] -> failwith "getaddrinfo"
  | h :: _ ->
    let buf = Bytes.create 1024 in
    let rec loop () =
      with_connection h (onConn ?creds buf host port);
      loop ()
    in
    (try loop () with
     | End_of_file -> ())
;;

let () =
  let err =
    lazy
      (Printf.eprintf "qcon host:port[:usr:pwd]\n";
       exit 1)
  in
  match Array.length Sys.argv with
  | 2 ->
    (match String.split_on_char ':' Sys.argv.(1) with
     | [ host; port ] -> qcon host port
     | [ host; port; user; passwd ] -> qcon ~creds:(user, passwd) host port
     | _ -> Lazy.force err)
  | _ -> Lazy.force err
;;
