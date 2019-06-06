/*---------------------------------------------------------------------------
   Copyright (c) 2018 Vincent Bernardoff. All rights reserved.
   Distributed under the ISC license, see terms at the end of the file.
   --------------------------------------------------------------------------*/

#include <string.h>
#include <errno.h>
#include <assert.h>
#include <caml/mlvalues.h>
#include <caml/alloc.h>
#include <caml/memory.h>
#include <caml/custom.h>
#include <caml/bigarray.h>
#include "k.h"

#define K_val(v) (*((K*) Data_custom_val(v)))
#define K_val_r1(v) (r1((*((K*) Data_custom_val(v)))))

static int compare_K(value a, value b) {
    K aa = K_val(a), bb = K_val(b);
    return (aa == bb ? 0 : (aa < bb ? -1 : 1));
}

/* #include <stdio.h> */
static void finalize_K(value k) {
    /* fprintf(stderr, "%p %d %d\n", K_val(k), K_val(k)->t, K_val(k)->r); */
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

static value caml_alloc_K (K a) {
    value custom = caml_alloc_custom(&kx_K_ops, sizeof(K), 0, 1);
    K_val(custom) = a;
    return custom;
}

CAMLprim value k_objtyp (value k) { return Val_int(K_val(k)->t); }
CAMLprim value k_objattrs (value k) { return Val_int(K_val(k)->u); }
CAMLprim value k_refcount (value k) { return Val_int(K_val(k)->r); }
CAMLprim value k_length (value k) { return Val_int(K_val(k)->n); }
CAMLprim value k_length64 (value k) { return caml_copy_int64(K_val(k)->n); }
CAMLprim value k_g (value k) { return Val_int(K_val(k)->g); }
CAMLprim value k_h (value k) { return Val_int(K_val(k)->h); }
CAMLprim value k_i_int (value k) { return Val_int(K_val(k)->i); }
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
    ret = caml_alloc_K(r1(K_val(k)->k));
    CAMLreturn(ret);
}

CAMLprim value k_u (value k) {
    CAMLparam1(k);
    CAMLlocal1(ret);
    ret = caml_alloc_initialized_string(16, K_val(k)->G0);
    CAMLreturn(ret);
}

/* List accessors */

CAMLprim value kK_stub (value k, value i) {
    CAMLparam2(k, i);
    CAMLlocal1(ret);
    ret = caml_alloc_K(r1(kK(K_val(k))[Long_val(i)]));
    CAMLreturn(ret);
}

CAMLprim value kK_set_stub (value k, value i, value e) {
    kK(K_val(k))[Int_val(i)] = r1(K_val(e));
    return Val_unit;
}

CAMLprim value kS_stub (value k, value i) {
    CAMLparam2(k, i);
    CAMLlocal1(ret);
    ret = caml_copy_string(kS(K_val(k))[Long_val(i)]);
    CAMLreturn(ret);
}

CAMLprim value kS_set_stub (value k, value i, value s) {
    kS(K_val(k))[Int_val(i)] = ss(String_val(s));
    return Val_unit;
}

CAMLprim value kG_stub (value k) {
    CAMLparam1(k);
    CAMLlocal1(ret);
    ret = caml_ba_alloc_dims(CAML_BA_UINT8 | CAML_BA_C_LAYOUT,
                             1, kG(K_val(k)), K_val(k)->n);
    CAMLreturn(ret);
}

CAMLprim value kU_stub (value k) {
    CAMLparam1(k);
    CAMLlocal1(ret);
    ret = caml_ba_alloc_dims(CAML_BA_UINT8 | CAML_BA_C_LAYOUT,
                             1, kU(K_val(k)), K_val(k)->n*16);
    CAMLreturn(ret);
}

CAMLprim value kH_stub (value k) {
    CAMLparam1(k);
    CAMLlocal1(ret);
    ret = caml_ba_alloc_dims(CAML_BA_SINT16 | CAML_BA_C_LAYOUT,
                             1, kH(K_val(k)), K_val(k)->n);
    CAMLreturn(ret);
}

CAMLprim value kI_stub (value k) {
    CAMLparam1(k);
    CAMLlocal1(ret);
    ret = caml_ba_alloc_dims(CAML_BA_INT32 | CAML_BA_C_LAYOUT,
                             1, kI(K_val(k)), K_val(k)->n);
    CAMLreturn(ret);
}

CAMLprim value kJ_stub (value k) {
    CAMLparam1(k);
    CAMLlocal1(ret);
    ret = caml_ba_alloc_dims(CAML_BA_INT64 | CAML_BA_C_LAYOUT,
                             1, kJ(K_val(k)), K_val(k)->n);
    CAMLreturn(ret);
}

CAMLprim value kE_stub (value k) {
    CAMLparam1(k);
    CAMLlocal1(ret);
    ret = caml_ba_alloc_dims(CAML_BA_FLOAT32 | CAML_BA_C_LAYOUT,
                             1, kE(K_val(k)), K_val(k)->n);
    CAMLreturn(ret);
}

CAMLprim value kF_stub (value k) {
    CAMLparam1(k);
    CAMLlocal1(ret);
    ret = caml_ba_alloc_dims(CAML_BA_FLOAT64 | CAML_BA_C_LAYOUT,
                             1, kF(K_val(k)), K_val(k)->n);
    CAMLreturn(ret);
}

/* Atom constructors */

CAMLprim value kb_stub (value b) {
    CAMLparam1(b);
    CAMLlocal1(k);
    k = caml_alloc_K(kb(Bool_val(b)));
    CAMLreturn(k);
}

CAMLprim value ku_stub (value u) {
    U uu;
    memcpy(uu.g, String_val(u), 16);
    CAMLparam1(u);
    CAMLlocal1(k);
    k = caml_alloc_K(ku(uu));
    CAMLreturn(k);
}

CAMLprim value kc_stub (value i) {
    CAMLparam1(i);
    CAMLlocal1(k);
    k = caml_alloc_K(kc(Int_val(i)));
    CAMLreturn(k);
}

CAMLprim value kg_stub (value i) {
    CAMLparam1(i);
    CAMLlocal1(k);
    k = caml_alloc_K(kg(Int_val(i)));
    CAMLreturn(k);
}

CAMLprim value kh_stub (value i) {
    CAMLparam1(i);
    CAMLlocal1(k);
    k = caml_alloc_K(kh(Int_val(i)));
    CAMLreturn(k);
}

CAMLprim value ki_stub (value i) {
    CAMLparam1(i);
    CAMLlocal1(k);
    k = caml_alloc_K(ki(Int32_val(i)));
    CAMLreturn(k);
}

CAMLprim value kj_stub (value i) {
    CAMLparam1(i);
    CAMLlocal1(k);
    k = caml_alloc_K(kj(Int64_val(i)));
    CAMLreturn(k);
}

CAMLprim value ke_stub (value f) {
    CAMLparam1(f);
    CAMLlocal1(k);
    k = caml_alloc_K(ke(Double_val(f)));
    CAMLreturn(k);
}

CAMLprim value kf_stub (value f) {
    CAMLparam1(f);
    CAMLlocal1(k);
    k = caml_alloc_K(kf(Double_val(f)));
    CAMLreturn(k);
}

CAMLprim value ks_stub (value s) {
    CAMLparam1(s);
    CAMLlocal1(k);
    k = caml_alloc_K(ks(String_val(s)));
    CAMLreturn(k);
}

CAMLprim value kp_stub (value s) {
    CAMLparam1(s);
    CAMLlocal1(k);
    k = caml_alloc_K(kp(String_val(s)));
    CAMLreturn(k);
}

CAMLprim value kpn_stub (value s, value n) {
    CAMLparam1(s);
    CAMLlocal1(k);
    k = caml_alloc_K(kpn(String_val(s), Int_val(n)));
    CAMLreturn(k);
}

CAMLprim value ktimestamp_stub (value j) {
    CAMLparam1(j);
    CAMLlocal1(k);
    k = caml_alloc_K(ktj(-KP, Int64_val(j)));
    CAMLreturn(k);
}

CAMLprim value kt_stub (value i) {
    CAMLparam1(i);
    CAMLlocal1(k);
    k = caml_alloc_K(kt(Int_val(i)));
    CAMLreturn(k);
}

CAMLprim value kd_stub (value i) {
    CAMLparam1(i);
    CAMLlocal1(k);
    k = caml_alloc_K(kd(Int_val(i)));
    CAMLreturn(k);
}

CAMLprim value kmonth_stub(value i) {
    CAMLparam1(i);
    CAMLlocal1(kk);
    K k = ka(-KM);
    k->i = Int_val(i);
    kk = caml_alloc_K(k);
    CAMLreturn(kk);
}
CAMLprim value kminute_stub(value i) {
    CAMLparam1(i);
    CAMLlocal1(kk);
    K k = ka(-KU);
    k->i = Int_val(i);
    kk = caml_alloc_K(k);
    CAMLreturn(kk);
}

CAMLprim value ksecond_stub(value i) {
    CAMLparam1(i);
    CAMLlocal1(kk);
    K k = ka(-KV);
    k->i = Int_val(i);
    kk = caml_alloc_K(k);
    CAMLreturn(kk);
}

CAMLprim value ktimespan_stub (value j) {
    CAMLparam1(j);
    CAMLlocal1(k);
    k = caml_alloc_K(ktj(-KN, Int64_val(j)));
    CAMLreturn(k);
}

CAMLprim value kz_stub (value f) {
    CAMLparam1(f);
    CAMLlocal1(k);
    k = caml_alloc_K(kz(Double_val(f)));
    CAMLreturn(k);
}

CAMLprim value ktn_stub (value t, value len) {
    CAMLparam2(t, len);
    CAMLlocal1(k);
    k = caml_alloc_K(ktn(Int_val(t), Int_val(len)));
    CAMLreturn(k);
}

CAMLprim value xD_stub (value keys, value values) {
    CAMLparam2(keys, values);
    CAMLlocal1(k);
    k = caml_alloc_K(xD(r1(K_val(keys)), r1(K_val(values))));
    CAMLreturn(k);
}

CAMLprim value xT_stub (value dict) {
    CAMLparam1(dict);
    CAMLlocal3(ret, msg, k);
    K kk = ee(xT(r1(K_val(dict))));
    if (kk->t == -128) {
        ret = caml_alloc(1, 1);
        msg = caml_copy_string(kk->s?kk->s:"");
        Store_field(ret, 0, msg);
    }
    else {
        ret = caml_alloc(1, 0);
        k = caml_alloc_K(kk);
        Store_field(ret, 0, k);
    }
    CAMLreturn(ret);
}

CAMLprim value d9_stub (value k) {
    CAMLparam1(k);
    CAMLlocal3(ret, errmsg, kk);
    K x = ee(d9(K_val(k)));
    if (xt == -128) {
        ret = caml_alloc(1, 1);
        errmsg = caml_copy_string(x->s?x->s:"");
        Store_field(ret, 0, errmsg);
    }
    else {
        ret = caml_alloc(1, 0);
        kk = caml_alloc_K(x);
        Store_field(ret, 0, kk);
    }
    CAMLreturn(ret);
}

CAMLprim value b9_stub (value mode, value k) {
    CAMLparam2(mode, k);
    CAMLlocal3(ret, errmsg, kk);
    K x = ee(b9(Int_val(mode), K_val(k)));
    assert(x->t == 4);
    if (xt == -128) {
        ret = caml_alloc(1, 1);
        errmsg = caml_copy_string(x->s?x->s:"");
        Store_field(ret, 0, errmsg);
    }
    else {
        ret = caml_alloc(1, 0);
        kk = caml_alloc_K(x);
        Store_field(ret, 0, kk);
    }
    CAMLreturn(ret);
}

CAMLprim value khp_stub (value host, value port) {
    return Val_int(khp(String_val(host), Int_val(port)));
}

CAMLprim value khpu_stub (value host, value port, value username) {
    return Val_int(khpu(String_val(host), Int_val(port), String_val(username)));
}

CAMLprim value khpun_stub(value host, value port,
                          value username, value timeout) {
    return Val_int(khpun(String_val(host), Int_val(port),
                         String_val(username), Int_val(timeout)));
}

CAMLprim value khpunc_stub(value host, value port,
                           value username, value timeout, value capability) {
    return Val_int(khpunc(String_val(host),
                         Int_val(port),
                         String_val(username),
                         Int_val(timeout),
                         Int_val(capability)));
}

CAMLprim value kclose_stub(value fd) {
    kclose(Int_val(fd));
    return Val_unit;
}

CAMLprim value kread_stub(value fd) {
    CAMLparam1(fd);
    CAMLlocal1(kk);
    K r = k(Int_val(fd), (S)NULL);
    kk = r ? caml_alloc_K(r) : caml_alloc_K(krr("Connection closed"));
    CAMLreturn(kk);
}

CAMLprim value k0_stub(value fd, value msg) {
    k(-Int_val(fd), String_val(msg), (K)NULL);
    return Val_unit;
}

CAMLprim value k1_stub(value fd, value msg, value a) {
    k(-Int_val(fd), String_val(msg), K_val_r1(a), (K)NULL);
    return Val_unit;
}

CAMLprim value k2_stub(value fd, value msg, value a, value b) {
    k(-Int_val(fd), String_val(msg), K_val_r1(a), K_val_r1(b), (K)NULL);
    return Val_unit;
}

CAMLprim value k3_stub(value fd, value msg, value a, value b, value c) {
    k(-Int_val(fd), String_val(msg), K_val_r1(a), K_val_r1(b), K_val_r1(c), (K)NULL);
    return Val_unit;
}

CAMLprim value k0_sync_stub(value fd, value msg) {
    CAMLparam2(fd, msg);
    CAMLlocal1(ret);
    K r = k(Int_val(fd), String_val(msg), (K)NULL);
    ret = caml_alloc_K(r);
    CAMLreturn(ret);
}

CAMLprim value k1_sync_stub(value fd, value msg, value a) {
    CAMLparam3(fd, msg, a);
    CAMLlocal1(ret);
    K r = k(Int_val(fd), String_val(msg), K_val_r1(a), (K)NULL);
    ret = caml_alloc_K(r);
    CAMLreturn(ret);
}

CAMLprim value k2_sync_stub(value fd, value msg, value a, value b) {
    CAMLparam4(fd, msg, a, b);
    CAMLlocal1(ret);
    K r = k(Int_val(fd), String_val(msg), K_val_r1(a), K_val_r1(b), (K)NULL);
    ret = caml_alloc_K(r);
    CAMLreturn(ret);
}

CAMLprim value k3_sync_stub(value fd, value msg, value a, value b, value c) {
    CAMLparam5(fd, msg, a, b, c);
    CAMLlocal1(ret);
    K r = k(Int_val(fd), String_val(msg), K_val_r1(a), K_val_r1(b), K_val_r1(c), (K)NULL);
    ret = caml_alloc_K(r);
    CAMLreturn(ret);
}

CAMLprim value kn_stub(value fd, value msg, value a) {
    switch (Wosize_val(a)) {
    case 0:
        k(-Int_val(fd), String_val(msg), (K)NULL);
        break;
    case 1:
        k(-Int_val(fd), String_val(msg),
          K_val_r1(Field(a, 0)), (K)NULL);
        break;
    case 2:
        k(-Int_val(fd), String_val(msg),
          K_val_r1(Field(a, 0)), K_val_r1(Field(a, 1)), (K)NULL);
        break;
    case 3:
        k(-Int_val(fd), String_val(msg),
          K_val_r1(Field(a, 0)), K_val_r1(Field(a, 1)),
          K_val_r1(Field(a, 2)), (K)NULL);
        break;
    case 4:
        k(-Int_val(fd), String_val(msg),
          K_val_r1(Field(a, 0)), K_val_r1(Field(a, 1)),
          K_val_r1(Field(a, 2)), K_val_r1(Field(a, 3)), (K)NULL);
        break;
    case 5:
        k(-Int_val(fd), String_val(msg),
          K_val_r1(Field(a, 0)), K_val_r1(Field(a, 1)),
          K_val_r1(Field(a, 2)), K_val_r1(Field(a, 3)),
          K_val_r1(Field(a, 4)), (K)NULL);
        break;
    case 6:
        k(-Int_val(fd), String_val(msg),
          K_val_r1(Field(a, 0)), K_val_r1(Field(a, 1)),
          K_val_r1(Field(a, 2)), K_val_r1(Field(a, 3)),
          K_val_r1(Field(a, 4)), K_val_r1(Field(a, 5)), (K)NULL);
        break;

    default:
        assert(0);
    }
    return Val_unit;
}

CAMLprim value kn_sync_stub(value fd, value msg, value a) {
    CAMLparam3(fd, msg, a);
    CAMLlocal1(ret);
    K r;
    switch (Wosize_val(a)) {
    case 0:
        r = k(Int_val(fd), String_val(msg), (K)NULL);
        break;
    case 1:
        r = k(Int_val(fd), String_val(msg),
              K_val_r1(Field(a, 0)), (K)NULL);
        break;
    case 2:
        r = k(Int_val(fd), String_val(msg),
              K_val_r1(Field(a, 0)), K_val_r1(Field(a, 1)), (K)NULL);
        break;
    case 3:
        r = k(Int_val(fd), String_val(msg),
              K_val_r1(Field(a, 0)), K_val_r1(Field(a, 1)),
              K_val_r1(Field(a, 2)), (K)NULL);
        break;
    case 4:
        r = k(Int_val(fd), String_val(msg),
              K_val_r1(Field(a, 0)), K_val_r1(Field(a, 1)),
              K_val_r1(Field(a, 2)), K_val_r1(Field(a, 3)), (K)NULL);
        break;
    case 5:
        r = k(Int_val(fd), String_val(msg),
              K_val_r1(Field(a, 0)), K_val_r1(Field(a, 1)),
              K_val_r1(Field(a, 2)), K_val_r1(Field(a, 3)),
              K_val_r1(Field(a, 4)), (K)NULL);
        break;
    case 6:
        r = k(Int_val(fd), String_val(msg),
              K_val_r1(Field(a, 0)), K_val_r1(Field(a, 1)),
              K_val_r1(Field(a, 2)), K_val_r1(Field(a, 3)),
              K_val_r1(Field(a, 4)), K_val_r1(Field(a, 5)), (K)NULL);
        break;
    }
    ret = caml_alloc_K(r);
    CAMLreturn(ret);
}

CAMLprim value ymd_stub(value year, value month, value day) {
    return Val_int(ymd(Int_val(year), Int_val(month), Int_val(day)));
}
CAMLprim value dj_stub(value i) {
    return Val_int(dj(Int_val(i)));
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
