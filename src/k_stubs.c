/*---------------------------------------------------------------------------
   Copyright (c) 2018 Vincent Bernardoff. All rights reserved.
   Distributed under the ISC license, see terms at the end of the file.
   --------------------------------------------------------------------------*/

#include <string.h>
#include <errno.h>
#include <caml/mlvalues.h>
#include <caml/alloc.h>
#include <caml/memory.h>
#include <caml/custom.h>
#include "k.h"

#define K_val(v) (*((K*) Data_custom_val(v)))

static int compare_K(value a, value b) {
    K aa = K_val(a), bb = K_val(b);
    return (aa == bb ? 0 : (aa < bb ? -1 : 1));
}

static void finalize_K(value k) {
    r0(K_val(k));
}

static struct custom_operations kx_K_ops =
    { .identifier = "kx_K",
      .finalize = finalize_K,
      .compare = compare_K,
      .compare_ext = custom_compare_ext_default,
      .hash = custom_hash_default,
      .serialize = custom_serialize_default,
      .deserialize = custom_deserialize_default };

static value alloc_K (K a) {
    value custom = caml_alloc_custom(&kx_K_ops, sizeof(K), 0, 1);
    K_val(custom) = a;
    return custom;
}

CAMLprim value k_objtyp (value k) { return Val_int(K_val(k)->t); }
CAMLprim value k_objattrs (value k) { return Val_int(K_val(k)->u); }
CAMLprim value k_refcount (value k) { return Val_int(K_val(k)->r); }
CAMLprim value k_length (value k) { return Val_int(K_val(k)->n); }
CAMLprim value k_g (value k) { return Val_int(K_val(k)->g); }
CAMLprim value k_h (value k) { return Val_int(K_val(k)->h); }
CAMLprim value k_i (value k) {
    CAMLparam1(k);
    CAMLlocal1(ret);
    ret = caml_copy_int32(K_val(k)->i);
    CAMLreturn(ret);
}
CAMLprim value k_j (value k) {
    CAMLparam1(k);
    CAMLlocal1(ret);
    ret = caml_copy_int64(K_val(k)->j);
    CAMLreturn(ret);
}
CAMLprim value k_e (value k) {
    CAMLparam1(k);
    CAMLlocal1(ret);
    ret = caml_copy_double(K_val(k)->e);
    CAMLreturn(ret);
}
CAMLprim value k_f (value k) {
    CAMLparam1(k);
    CAMLlocal1(ret);
    ret = caml_copy_double(K_val(k)->f);
    CAMLreturn(ret);
}
CAMLprim value k_s (value k) {
    CAMLparam1(k);
    CAMLlocal1(ret);
    ret = caml_copy_string(K_val(k)->s);
    CAMLreturn(ret);
}
CAMLprim value k_k (value k) {
    CAMLparam1(k);
    CAMLlocal1(ret);
    ret = alloc_K(K_val(k)->k);
    CAMLreturn(ret);
}

CAMLprim value kb_stub (value b) {
    CAMLparam1(b);
    CAMLlocal1(k);
    k = alloc_K(kb(Bool_val(b)));
    CAMLreturn(k);
}

CAMLprim value ku_stub (value u) {
    U uu;
    memcpy(uu.g, String_val(u), 16);
    CAMLparam1(u);
    CAMLlocal1(k);
    k = alloc_K(ku(uu));
    CAMLreturn(k);
}

CAMLprim value kc_stub (value i) {
    CAMLparam1(i);
    CAMLlocal1(k);
    k = alloc_K(kc(Int_val(i)));
    CAMLreturn(k);
}
CAMLprim value kg_stub (value i) {
    CAMLparam1(i);
    CAMLlocal1(k);
    k = alloc_K(kg(Int_val(i)));
    CAMLreturn(k);
}
CAMLprim value kh_stub (value i) {
    CAMLparam1(i);
    CAMLlocal1(k);
    k = alloc_K(kh(Int_val(i)));
    CAMLreturn(k);
}
CAMLprim value ki_stub (value i) {
    CAMLparam1(i);
    CAMLlocal1(k);
    k = alloc_K(ki(Int32_val(i)));
    CAMLreturn(k);
}
CAMLprim value kj_stub (value i) {
    CAMLparam1(i);
    CAMLlocal1(k);
    k = alloc_K(kj(Int64_val(i)));
    CAMLreturn(k);
}

CAMLprim value ke_stub (value f) {
    CAMLparam1(f);
    CAMLlocal1(k);
    k = alloc_K(ke(Double_val(f)));
    CAMLreturn(k);
}
CAMLprim value kf_stub (value f) {
    CAMLparam1(f);
    CAMLlocal1(k);
    k = alloc_K(kf(Double_val(f)));
    CAMLreturn(k);
}

CAMLprim value ks_stub (value s) {
    CAMLparam1(s);
    CAMLlocal1(k);
    k = alloc_K(ks(String_val(s)));
    CAMLreturn(k);
}
CAMLprim value kp_stub (value s) {
    CAMLparam1(s);
    CAMLlocal1(k);
    k = alloc_K(kp(String_val(s)));
    CAMLreturn(k);
}
CAMLprim value kpn_stub (value s, value n) {
    CAMLparam1(s);
    CAMLlocal1(k);
    k = alloc_K(kpn(String_val(s), Int_val(n)));
    CAMLreturn(k);
}

CAMLprim value ktimestamp_stub (value j) {
    CAMLparam1(j);
    CAMLlocal1(k);
    k = alloc_K(ktj(-KP, Int64_val(j)));
    CAMLreturn(k);
}
CAMLprim value kt_stub (value i) {
    CAMLparam1(i);
    CAMLlocal1(k);
    k = alloc_K(kt(Int32_val(i)));
    CAMLreturn(k);
}
CAMLprim value kd_stub (value i) {
    CAMLparam1(i);
    CAMLlocal1(k);
    k = alloc_K(kd(Int32_val(i)));
    CAMLreturn(k);
}

CAMLprim value kmonth_stub(value i) {
    CAMLparam1(i);
    CAMLlocal1(kk);
    K k = ka(-KM);
    k->i = Int32_val(i);
    kk = alloc_K(k);
    CAMLreturn(kk);
}
CAMLprim value kminute_stub(value i) {
    CAMLparam1(i);
    CAMLlocal1(kk);
    K k = ka(-KU);
    k->i = Int32_val(i);
    kk = alloc_K(k);
    CAMLreturn(kk);
}
CAMLprim value ksecond_stub(value i) {
    CAMLparam1(i);
    CAMLlocal1(kk);
    K k = ka(-KV);
    k->i = Int32_val(i);
    kk = alloc_K(k);
    CAMLreturn(kk);
}

CAMLprim value ktimespan_stub (value j) {
    CAMLparam1(j);
    CAMLlocal1(k);
    k = alloc_K(ktj(-KN, Int64_val(j)));
    CAMLreturn(k);
}

CAMLprim value kz_stub (value f) {
    CAMLparam1(f);
    CAMLlocal1(k);
    k = alloc_K(kz(Double_val(f)));
    CAMLreturn(k);
}

CAMLprim value ktn_stub (value t, value len) {
    CAMLparam2(t, len);
    CAMLlocal1(k);
    k = alloc_K(ktn(Int_val(t), Long_val(len)));
    CAMLreturn(k);
}

CAMLprim value ja_int_stub (value k, value i) {
    int ii = Int_val(i);
    K ret = ja(Data_custom_val(k), &ii);
    K_val(k) = ret;
    return Val_unit;
}

CAMLprim value ja_long_stub (value k, value j) {
    int jj = Long_val(j);
    K ret = ja(Data_custom_val(k), &jj);
    K_val(k) = ret;
    return Val_unit;
}

CAMLprim value ja_int32_stub (value k, value i) {
    int ii = Int32_val(i);
    K ret = ja(Data_custom_val(k), &ii);
    K_val(k) = ret;
    return Val_unit;
}

CAMLprim value ja_int64_stub (value k, value j) {
    long jj = Int64_val(j);
    K ret = ja(Data_custom_val(k), &jj);
    K_val(k) = ret;
    return Val_unit;
}

CAMLprim value ja_double_stub (value k, value f) {
    double ff = Double_val(f);
    K ret = ja(Data_custom_val(k), &ff);
    K_val(k) = ret;
    return Val_unit;
}

CAMLprim value ja_bool_stub (value k, value b) {
    int bb = Bool_val(b);
    K ret = ja(Data_custom_val(k), &bb);
    K_val(k) = ret;
    return Val_unit;
}

CAMLprim value ja_uuid_stub(value k, value u) {
    K ret = ja(Data_custom_val(k), String_val(u));
    K_val(k) = ret;
    return Val_unit;
}

CAMLprim value js_stub (value k, value s) {
    K ret = js(Data_custom_val(k), ss(String_val(s)));
    K_val(k) = ret;
    return Val_unit;
}

CAMLprim value jk_stub (value k, value v) {
    K ret = jk(Data_custom_val(k), K_val(v));
    K_val(k) = ret;
    return Val_unit;
}

CAMLprim value jv_stub (value k, value v) {
    K ret = jv(Data_custom_val(k), K_val(v));
    K_val(k) = ret;
    return Val_unit;
}

CAMLprim value khpu_stub (value host, value port, value username) {
    CAMLparam3(host, port, username);
    CAMLlocal2(ret, errmsg);
    int rc = khpu(String_val(host), Int_val(port), String_val(username));
    if (rc > 0) {
        ret = caml_alloc(1, 0);
        Store_field(ret, 0, Val_int(rc));
    }
    else {
        ret = caml_alloc(1, 1);
        errmsg = caml_copy_string(strerror(errno));
        Store_field(ret, 0, errmsg);
    }
    CAMLreturn(ret);
}

CAMLprim value khpun_stub(value host, value port,
                          value username, value timeout) {
    return Val_int(khpun(String_val(host), Int_val(port),
                         String_val(username), Int_val(timeout)));
}

CAMLprim value k0_stub(value conn, value msg) {
    CAMLparam2(conn, msg);
    CAMLlocal3(ret, errmsg, kk);
    K r = k(Int_val(conn), String_val(msg), (K)0);
    if (!r) {
        ret = caml_alloc(1, 1);
        errmsg = caml_copy_string(strerror(errno));
        Store_field(ret, 0, errmsg);
    }
    else {
        ret = caml_alloc(1, 0);
        kk = alloc_K(r);
        Store_field(ret, 0, kk);
    }
    CAMLreturn(ret);
}

CAMLprim value k1_stub(value conn, value msg, value a) {
    CAMLparam3(conn, msg, a);
    CAMLlocal3(ret, errmsg, kk);
    K r = k(Int_val(conn), String_val(msg), K_val(a), (K)0);
    if (!r) {
        ret = caml_alloc(1, 1);
        errmsg = caml_copy_string(strerror(errno));
        Store_field(ret, 0, errmsg);
    }
    else {
        ret = caml_alloc(1, 0);
        kk = alloc_K(r);
        Store_field(ret, 0, kk);
    }
    CAMLreturn(ret);
}

/*---------------------------------------------------------------------------
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
  ---------------------------------------------------------------------------*/
