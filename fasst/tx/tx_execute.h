#ifndef TX_EXECUTE_H
#define TX_EXECUTE_H

#include "datastore/ds.h"
#include "tx.h"
#include "tx_defs.h"

#define forceinline inline __attribute__((always_inline))

/* Read keys */
forceinline tx_status_t Tx::do_read(coro_yield_t &yield) {
  tx_dassert(tx_status == tx_status_t::in_progress);

  tx_dassert(read_set.size() + write_set.size() <= MAX_INNER_MSGS_COALESCED &&
             read_set.size() + write_set.size() > 0);
  tx_dassert(rs_index <= read_set.size());
  tx_dassert(ws_index <= write_set.size());

#if TX_ENABLE_LOCK_SERVER == 1
  if (mappings->use_lock_server) {
    tx_stat_inc(stat_lockserver_lock_req, 1);
    bool lock_success = send_lockserver_req(yield, locksrv_reqtype_t::lock);
    if (!lock_success) {
      tx_status = tx_status_t::must_abort;
      return tx_status;
    } else {
      tx_stat_inc(stat_lockserver_lock_req_success, 1);
      /* Record so we know whether to unlock on abort */
      lockserver_locked = true;
    }
  }
#endif

  size_t req_i = 0;
  rpc_cmsg_t *req_cmsg = rpc->start_req_cmsg(thread_id);

  /* Read the read set */
  for (size_t i = rs_index; i < read_set.size(); i++) {
    tx_rwset_item_t &item = read_set[i];
    rpc_inner_msg_t *req_inner_msg = rpc->start_inner_msg(
        req_cmsg, item.rpc_reqtype, sizeof(ds_generic_get_req_t),
        (uint8_t *)&item.obj->hdr);
    ds_forge_generic_get_req(req_inner_msg, global_client_id, item.key,
                             item.keyhash, ds_reqtype_t::get_rdonly);
    req_i++;
  }

  /* Read + lock the write set */
  for (size_t i = ws_index; i < write_set.size(); i++) {
    tx_rwset_item_t &item = write_set[i];
    rpc_inner_msg_t *req_inner_msg = rpc->start_inner_msg(
        req_cmsg, item.rpc_reqtype, sizeof(ds_generic_get_req_t),
        (uint8_t *)&item.obj->hdr);
    /* In the execute phase, update and delete keys are handled similarly */
    if (item.write_mode != tx_write_mode_t::insert) {
      /* Update or delete */
      ds_forge_generic_get_req(req_inner_msg, global_client_id, item.key,
                               item.keyhash, ds_reqtype_t::get_for_upd);
    } else {
      /* Insert */
      ds_forge_generic_get_req(req_inner_msg, global_client_id, item.key,
                               item.keyhash, ds_reqtype_t::lock_for_ins);
    }
    req_i++;
  }

  tx_dassert(req_i > 0 && req_i <= MAX_INNER_MSGS_COALESCED);

  int posted = rpc->post_req_cmsgs();
  tx_wait_till_ready(yield, posted);

  req_i = 0;

  /*
   * a. Sanity-check the response.
   * b. Record read set versions for validation.
   * c. Record locking status of all write set keys to unlock on abort.
   */
  rpc_cmsg_t *resp_cmsg = rpc->get_resp_cmsg(0);
  rpc_inner_msg_t *inner_msg = (rpc_inner_msg_t *)(resp_cmsg + 1);
  for (size_t i = rs_index; i < read_set.size(); i++) {
    tx_rwset_item_t &item = read_set[i];
    ds_resptype_t resp_type = (ds_resptype_t)inner_msg->resp_type;

    /* Hdr for successfully read keys need not be locked (bkt collison) */
    switch (resp_type) {
      case ds_resptype_t::get_rdonly_success:
        /* Response contains header and value */
        item.obj->val_size = inner_msg->data_len - sizeof(hots_hdr_t);
        check_item(item); /* Checks @val_size */

        /* Save fields needed for validation */
        item.exec_rs_exists = true;
        item.exec_rs_version = item.obj->hdr.version;
        break;
      case ds_resptype_t::get_rdonly_not_found:
        /* Txn need not be aborted if a rdonly key is not found. */
        tx_dassert(inner_msg->data_len == sizeof(uint64_t));

        item.obj->val_size = 0;

        /* Save fields needed for validation */
        item.exec_rs_exists = false;
        item.exec_rs_version = item.obj->hdr.version;
        break;
      case ds_resptype_t::get_rdonly_locked:
        tx_dassert(inner_msg->data_len == 0);
        tx_status = tx_status_t::must_abort;
        break;
      default:
        printf(
            "Tx: Unknown response type %u for read set key "
            "%" PRIu64 "\n.",
            inner_msg->resp_type, item.key);
    }

    req_i++;
    inner_msg =
        (rpc_inner_msg_t *)((uint8_t *)(inner_msg + 1) + inner_msg->data_len);
  }

  for (size_t i = ws_index; i < write_set.size(); i++) {
    tx_rwset_item_t &item = write_set[i];
    ds_resptype_t resp_type = (ds_resptype_t)inner_msg->resp_type;

    if (item.write_mode != tx_write_mode_t::insert) {
      // Update or delete
      switch (resp_type) {
        case ds_resptype_t::get_for_upd_success:
          tx_dassert(item.obj->hdr.locked == 1);

          item.obj->val_size = inner_msg->data_len - sizeof(hots_hdr_t);
          check_item(item); /* Checks @val_size */

          item.exec_ws_locked = true; /* Mark for unlock on abort */
          break;
        case ds_resptype_t::get_for_upd_not_found:
        case ds_resptype_t::get_for_upd_locked:
          tx_dassert(inner_msg->data_len == 0);

          item.exec_ws_locked = false; /* Don't unlock on abort */
          tx_status = tx_status_t::must_abort;
          break;
        default:
          printf(
              "Tx: Unknown response type %u for write set "
              "(non-insert) key %" PRIu64 "\n.",
              inner_msg->resp_type, item.key);
          exit(-1);
      }
    } else {
      // Insert
      switch (resp_type) {
        case ds_resptype_t::lock_for_ins_success:
          tx_dassert(item.obj->hdr.locked == 1);
          tx_dassert(inner_msg->data_len ==
                     sizeof(hots_hdr_t)); /* Just the header */
          item.exec_ws_locked = true;     /* Mark for delete on abort */
          break;
        case ds_resptype_t::lock_for_ins_exists:
        case ds_resptype_t::lock_for_ins_locked:
          tx_dassert(inner_msg->data_len == 0);
          tx_status = tx_status_t::must_abort;
          item.exec_ws_locked = false; /* Don't unlock on abort */
          break;
        default:
          printf(
              "Tx: Unknown response type %u for write set "
              "(insert) key %" PRIu64 "\n.",
              inner_msg->resp_type, item.key);
          exit(-1);
      }
    }

    req_i++;
    inner_msg =
        (rpc_inner_msg_t *)((uint8_t *)(inner_msg + 1) + inner_msg->data_len);
  }

  /*
   * These indices only make sense if we return ex_success, so no need to
   * update them in error cases.
   */
  rs_index = read_set.size();
  ws_index = write_set.size();

  tx_dassert(tx_status == tx_status_t::in_progress ||
             tx_status == tx_status_t::must_abort);
  return tx_status;
}

#endif /* TX_EXECUTE_H */
