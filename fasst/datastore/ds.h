#ifndef DS_H
#define DS_H

#include <cassert>
#include <cstdint>

#include "hots.h"
#include "mica/util/hash.h"
#include "rdmax/rpc.h"
#include "util/rte_memcpy.h"

using namespace rdmax;

// General datastore defines
#define ds_dassert(x)               \
  do {                              \
    if (DS_DEBUG_ASSERT) assert(x); \
  } while (0)
#define ds_dassert_msg(x, msg)                    \
  do {                                            \
    if (DS_DEBUG_ASSERT) HOTS_ASSERT_MSG(x, msg); \
  } while (0)

#define _unused(x) ((void)(x)) /* Make production build happy */
#define forceinline inline __attribute__((always_inline))
/* Is pointer x aligned to A-byte alignment? */
#define is_aligned(x, A) (((uint64_t)x) % A == 0)

/* We only need 48-bit key hash in requests; saved bits are used for req type */
#define ds_hashmask ((1ull << 48) - 1)
static uint64_t ds_keyhash(hots_key_t key) {
  return ::mica::util::hash((char *)&key, sizeof(hots_key_t)) & ds_hashmask;
}

/* IMPORTANT: GET request should be a prefix of PUT request */
struct ds_generic_get_req_t {
  uint32_t unused; /* Make struct size multiple of 8 bytes */
  uint32_t caller_id;
  uint64_t req_type : 4;
  uint64_t unused_val_size : 12; /* This field is used in PUT reqs */
  uint64_t keyhash : 48;         /* 16 bytes up to here */
  hots_key_t key;
};
static_assert(sizeof(ds_generic_get_req_t) == 3 * sizeof(uint64_t), "");

// Generic PUT requests are used when the object's value value is needed in the
// request. This includes only commit-time PUT requests.
struct ds_generic_put_req_t {
  uint32_t unused; /* Make struct size multiple of 8 bytes */
  uint32_t caller_id;
  uint64_t req_type : 4;
  uint64_t val_size : 12;
  uint64_t keyhash : 48; /* 16 bytes up to here */
  hots_key_t key;
  /* Identical to ds_generic_get_req_t up to here */
  uint8_t val[HOTS_MAX_VALUE];
};
static_assert(sizeof(ds_generic_put_req_t) ==
                  sizeof(ds_generic_get_req_t) + HOTS_MAX_VALUE,
              "");

/*
 * For a given value size, a PUT request is the largest among all of ds_*
 * requests and responses. (A GET response only contains the header and the
 * value, whereas a PUT request contains the caller ID, keyhash, key, and value.
 */
#define ds_put_req_size(val_sz) \
  (sizeof(ds_generic_put_req_t) - HOTS_MAX_VALUE + val_sz)

/* Request type is sent in the application-level payload in the RPC. */
enum class ds_reqtype_t {
  // Sent using generic GET request
  get_rdonly,   /* Just GET */
  get_for_upd,  /* GET for transaction update */
  lock_for_ins, /* Lock a bucket for insert */
  del,          /* Delete */
  unlock,       /* Unlock a bucket */

  // Sent using generic PUT request
  put, /* Insert or update */

  /*
   * Max 16 req types (4 bits) because of bitfield sizing in
   * ds_generic_get_req_t and ds_generic_put_req_t. The RPC subsystem's header
   * allows for 65536 response types, so we only need to change the ds_generic
   * structs if we need more types.
   */
};

/* Resp type is sent in the RPC's coalesced message request (response) header */
enum class ds_resptype_t {
  get_rdonly_success,
  get_rdonly_not_found,
  get_rdonly_locked,

  get_for_upd_success,
  get_for_upd_not_found,
  get_for_upd_locked,

  lock_for_ins_success,
  lock_for_ins_exists,
  lock_for_ins_locked,

  unlock_success,

  put_success,
  del_success,

  /* Max 16 resp types (4 bits). See comment for ds_reqtype_t above. */
};

/* Datastore request format checks : done once ever */
static void ds_do_checks() {
  /*
   * Check if the keyhash field of both GET and PUT requests is aligned. This
   * is required for prefetching in the RPC subsystem.
   */
  ds_generic_get_req_t gg_req;
  ds_generic_put_req_t *gp_req = (ds_generic_put_req_t *)&gg_req;
  gg_req.keyhash = 2207; /* Some magic number */
  assert(gg_req.keyhash == gp_req->keyhash);
  _unused(gg_req);
  _unused(gp_req);
}

/* Forge a GET request. Return size of the request. */
forceinline size_t ds_forge_generic_get_req(rpc_inner_msg_t *inner_msg,
                                            uint32_t caller_id, hots_key_t key,
                                            uint64_t keyhash,
                                            ds_reqtype_t req_type) {
  ds_dassert(req_type == ds_reqtype_t::get_rdonly ||
             req_type == ds_reqtype_t::get_for_upd ||
             req_type == ds_reqtype_t::lock_for_ins ||
             req_type == ds_reqtype_t::del || req_type == ds_reqtype_t::unlock);

  ds_dassert(inner_msg != NULL);
  ds_dassert(is_aligned(inner_msg, sizeof(uint32_t)));

  {
    /* Real work */
    ds_generic_get_req_t *gg_req = (ds_generic_get_req_t *)(inner_msg + 1);
    gg_req->caller_id = caller_id;
    gg_req->req_type = static_cast<uint64_t>(req_type);
    gg_req->keyhash = keyhash;
    gg_req->key = key;
    return sizeof(ds_generic_get_req_t);
  }
}

/* Forge a PUT request. Return size of the request. */
forceinline size_t ds_forge_generic_put_req(rpc_inner_msg_t *inner_msg,
                                            uint32_t caller_id, hots_key_t key,
                                            uint64_t keyhash, hots_obj_t *obj,
                                            ds_reqtype_t req_type) {
  ds_dassert(req_type == ds_reqtype_t::put);
  ds_dassert(inner_msg != NULL);
  ds_dassert(obj != NULL);
  ds_dassert(is_aligned(inner_msg, sizeof(uint32_t)));
  ds_dassert(obj->val_size > 0 && obj->val_size % sizeof(uint64_t) == 0);
  size_t req_len = ds_put_req_size(obj->val_size);

  {
    /* Real work */
    ds_generic_put_req_t *gp_req = (ds_generic_put_req_t *)(inner_msg + 1);

    gp_req->caller_id = caller_id;
    gp_req->req_type = static_cast<uint64_t>(req_type);
    gp_req->val_size = obj->val_size;
    gp_req->key = key;
    gp_req->keyhash = keyhash;

    size_t obj_size = hots_obj_size(obj->val_size);
    rte_memcpy((void *)&gp_req->val, (void *)obj->val, obj_size);

    return req_len;
  }
}

#endif  // DS_H
