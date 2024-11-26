#ifndef TX_COMMIT_H
#define TX_COMMIT_H

#include "tx.h"

#define forceinline inline __attribute__((always_inline))

/* Send update messages to all replicas in @replica_vec in one batch */
forceinline void Tx::send_updates_to_replicas(coro_yield_t &yield,
                                              std::vector<int> replica_vec) {
  /* The largest replica set that @replica_vec can contain is all backups */
  //   tx_dassert(replica_vec.size() >= 1 && replica_vec.size() <=
  //   HOTS_MAX_BACKUPS);
  tx_dassert(replica_vec.size() == 1);

  size_t req_i = 0;

  for (int repl_i : replica_vec) {
    tx_dassert(repl_i >= 0 && repl_i < mappings->num_replicas);

    rpc_cmsg_t *req_cmsg = rpc->start_req_cmsg(thread_id);

    for (size_t w_i = 0; w_i < write_set.size(); w_i++) {
      tx_rwset_item_t &item = write_set[w_i];

      int repl_mn =
          (repl_i == 0) ? item.primary_mn : item.backup_mn[repl_i - 1];
      uint16_t rpc_reqtype = item.rpc_reqtype + repl_i;

      //   rpc_req_t *req = rpc->start_new_req(coro_id, rpc_reqtype, repl_mn,
      //                                       (uint8_t *)item.obj->val,
      //                                       sizeof(uint64_t)); /* Small resp
      //                                       */
      rpc_inner_msg_t *req_inner_msg =
          rpc->start_inner_msg(req_cmsg, rpc_reqtype, 0);  // set size latter

      tx_dassert(req_i < WORKER_WINDOW_SIZE);
      req_i++;

      size_t size_req;
      if (item.write_mode != tx_write_mode_t::del) {
        /* Insert or update */
        size_req =
            ds_forge_generic_put_req(req_inner_msg, global_client_id, item.key,
                                     item.keyhash, item.obj, ds_reqtype_t::put);
      } else {
        /* Delete */
        size_req =
            ds_forge_generic_get_req(req_inner_msg, global_client_id, item.key,
                                     item.keyhash, ds_reqtype_t::del);
      }
      req_inner_msg->data_len = size_req;
    }
  }

  //   tx_dassert(req_i <= WORKER_WINDOW_SIZE);
  int posted = rpc->post_req_cmsgs();
  tx_wait_till_ready(yield, posted);

  /* Check the responses */
  rpc_cmsg_t *resp_cmsg = rpc->get_resp_cmsg(0);
  rpc_inner_msg_t *inner_msg = (rpc_inner_msg_t *)(resp_cmsg + 1);
  for (size_t _req_i = 0; _req_i < req_i; _req_i++) {
    uint16_t resp_type = inner_msg->resp_type;
    _unused(resp_type);
    tx_dassert(resp_type == (uint16_t)ds_resptype_t::put_success ||
               resp_type == (uint16_t)ds_resptype_t::del_success);
    inner_msg =
        (rpc_inner_msg_t *)((uint8_t *)(inner_msg + 1) + inner_msg->data_len);
  }
}

/* Run the commit phase of this transaction */
forceinline tx_status_t Tx::commit(coro_yield_t &yield) {
  tx_dassert(tx_status == tx_status_t::in_progress);

  if (TX_ENABLE_LOCK_SERVER == 1 && mappings->use_lock_server) {
    /* If we're using lockserver, we must have successfully locked */
    tx_dassert(lockserver_locked);
  } else {
    // We only need to validate if we're not using lock server
    if (read_set.size() > 0) {
      /* Only invoke validation if there's a non-empty read set. */
      bool validation_success = validate(yield);
      if (!validation_success) {
        abort(yield);
        tx_dassert(tx_status == tx_status_t::aborted);
        return tx_status_t::aborted;
      }
    }
  }

  /* Invoke later phases only if there's a non-empty write set. */
  if (write_set.size() == 0) {
    tx_status = tx_status_t::committed;

#if TX_ENABLE_LOCK_SERVER == 1
    /* We still need to unlock the read set at lock server */
    if (mappings->use_lock_server) {
      /* lockserver_locked must be true bc we're committing */
      tx_stat_inc(stat_lockserver_unlock_req, 1);
      send_lockserver_req(yield, locksrv_reqtype_t::unlock);
    }
#endif

    return tx_status_t::committed;
  }

  // If we are here, validation has succeeded and we have a non-empty write
  // set. Since we have locks on the write set, commit should succeed now.

  /* Do logging */
  //   if (mappings->num_backups > 0) {
  //     log(yield);
  //   }

  // Send update requests to backups first and wait for ACKs. After receiving
  // ACKs, send updates to to primary.
  std::vector<int> replica_vec(HOTS_MAX_BACKUPS);
  replica_vec.clear();

  if (mappings->num_backups > 0) {
    /* Can we send the (W * f) messages to backups in one batch? */
    bool backups_in_one_batch =
        (write_set.size() * mappings->num_backups) <= WORKER_WINDOW_SIZE;

    // Send to backups
    if (backups_in_one_batch) {
      /* Send to all backups in one batch */
      for (int repl_i = 1; repl_i < mappings->num_replicas; repl_i++) {
        replica_vec.push_back(repl_i);
      }
      send_updates_to_replicas(yield, replica_vec);
    } else {
      /* Send to each backup in a separate batch */
      for (int repl_i = 1; repl_i < mappings->num_replicas; repl_i++) {
        replica_vec.clear();
        replica_vec.push_back(repl_i);
        send_updates_to_replicas(yield, replica_vec);
      }
    }
  }

  // Send to primary
  replica_vec.clear();
  replica_vec.push_back(0); /* Push the primary's replica number */
  send_updates_to_replicas(yield, replica_vec);

  tx_status = tx_status_t::committed;

#if TX_ENABLE_LOCK_SERVER == 1
  if (mappings->use_lock_server) {
    tx_stat_inc(stat_lockserver_unlock_req, 1);
    send_lockserver_req(yield, locksrv_reqtype_t::unlock);
  }
#endif

  return tx_status_t::committed;
}

/* Abort this transaction by unlocking locked keys and deleting temp objects */
forceinline void Tx::abort(coro_yield_t &yield) {
  /* User cannot call abort() after calling commit() or abort() */
  tx_dassert(tx_status == tx_status_t::in_progress ||
             tx_status == tx_status_t::must_abort);

  tx_status = tx_status_t::aborted;

#if TX_ENABLE_LOCK_SERVER == 1
  if (mappings->use_lock_server) {
    if (lockserver_locked) {
      /* Only unlock if we successfully locked in the 1st place */
      tx_stat_inc(stat_lockserver_unlock_req, 1);
      send_lockserver_req(yield, locksrv_reqtype_t::unlock);
      return;
    } else {
      /* No need to unlock anything bc lockserver unlocks on failure. */
      return;
    }
  }
#endif

  /*
   * If we are here, lockserver is not being used so we need to individually
   * unlock the write set keys that we successfully locked. Insert mode keys
   * that were successfully (temporarily) inserted were also marked locked
   * during execution.
   */

  size_t req_i = 0; /* Separate index bc we will skip some write set keys */
  rpc_cmsg_t *req_cmsg = rpc->start_req_cmsg(thread_id);

  for (size_t i = 0; i < write_set.size(); i++) {
    req_i++;
    tx_rwset_item_t &item = write_set[i];
    if (!item.exec_ws_locked) {
      continue;
    }
    rpc_inner_msg_t *req_inner_msg = rpc->start_inner_msg(
        req_cmsg, item.rpc_reqtype, sizeof(ds_generic_get_req_t));
    ds_forge_generic_get_req(req_inner_msg, global_client_id, item.key,
                             item.keyhash, ds_reqtype_t::unlock);
  }

  /* Nothing to unlock */
  if (req_i == 0) {
    return;
  }

  int posted = rpc->post_req_cmsgs();
  tx_wait_till_ready(yield, posted);

  req_i = 0;

  /* Check the response */
  rpc_cmsg_t *resp_cmsg = rpc->get_resp_cmsg(0);
  rpc_inner_msg_t *inner_msg = (rpc_inner_msg_t *)(resp_cmsg + 1);
  for (size_t i = 0; i < write_set.size(); i++) {
    tx_rwset_item_t &item = write_set[i];
    if (!item.exec_ws_locked) {
      continue;
    }
    tx_dassert(inner_msg->resp_type == (uint16_t)ds_resptype_t::unlock_success);
    req_i++;
    inner_msg =
        (rpc_inner_msg_t *)((uint8_t *)(inner_msg + 1) + inner_msg->data_len);
  }
}

/* Validate keys. For now, we re-read full objects during validation. */
bool Tx::validate(coro_yield_t &yield) {
  tx_dassert(read_set.size() > 0 && read_set.size() <= WORKER_WINDOW_SIZE);

  /* Send requests */
  rpc_cmsg_t *req_cmsg = rpc->start_req_cmsg(thread_id);

  for (size_t i = 0; i < read_set.size(); i++) {
    tx_rwset_item_t &item = read_set[i];

    /* The object should be sane if it existed during execute */
    if (item.exec_rs_exists) {
      tx_dassert(item.obj != NULL);
      tx_dassert(item.obj->hdr.canary == HOTS_VERSION_CANARY);
    }

    /*
     * Use the object as the validation response destination - this
     * corrupts the application object, but that's OK since the app gives
     * up the buffer on calling commit().
     */
    rpc_inner_msg_t *req_inner_msg = rpc->start_inner_msg(
        req_cmsg, item.rpc_reqtype, sizeof(ds_generic_get_req_t));
    ds_forge_generic_get_req(req_inner_msg, global_client_id, item.key,
                             item.keyhash, ds_reqtype_t::get_rdonly);
  }

  int posted = rpc->post_req_cmsgs();
  tx_wait_till_ready(yield, posted);

  // Check the response.
  // The loop below may only return false; true can be returned only after
  // inspecting all keys.
  rpc_cmsg_t *resp_cmsg = rpc->get_resp_cmsg(0);
  rpc_inner_msg_t *inner_msg = (rpc_inner_msg_t *)(resp_cmsg + 1);
  for (size_t i = 0; i < read_set.size(); i++) {
    tx_rwset_item_t &item = read_set[i];
    ds_resptype_t resp_type = (ds_resptype_t)inner_msg->resp_type;
    tx_dassert(resp_type == ds_resptype_t::get_rdonly_success ||
               resp_type == ds_resptype_t::get_rdonly_not_found ||
               resp_type == ds_resptype_t::get_rdonly_locked);

    if (item.exec_rs_exists) {
      // This key existed during execution
      if (resp_type == ds_resptype_t::get_rdonly_success) {
        /* If it still exists, sanity-check the object */
        tx_dassert(item.obj->hdr.canary == HOTS_VERSION_CANARY);
        tx_dassert(item.obj->val_size ==
                   inner_msg->data_len -
                       sizeof(hots_hdr_t)); /* Size cannot have changed */

        /*
         * If the object has a different version now, fail.
         * IMPORTANT: This check ignores the "locked" bit of the
         * header. If the key's bucket was locked but the datastore
         * returned success, it means that the bucket was locked by this
         * coroutine, so validation should succeed.
         */
        if (item.obj->hdr.version != item.exec_rs_version) {
          return false;
        }
      } else {
        /*
         * The object is either locked (get_rdonly_locked), or it has
         * been deleted since we read it (get_rdonly_not_found).
         */
        return false;
      }
    } else {
      /* A key that didn't exist during execute shouldn't exist now */
      if (resp_type == ds_resptype_t::get_rdonly_not_found) {
        tx_dassert(inner_msg->data_len == sizeof(uint64_t));

        /*
         * We ignore the "locked" bit here. See comment for
         * successfully-read keys above.
         */
        if (item.obj->hdr.version != item.exec_rs_version) {
          /*
           * This key, or some other key in this key's bucket, was
           * inserted and deleted by other transactions.
           */
          return false;
        }

      } else {
        /*
         * The key either exists now, or its bucket is locked by some
         * other coroutine.
         */
        return false;
      }
    }
    inner_msg =
        (rpc_inner_msg_t *)((uint8_t *)(inner_msg + 1) + inner_msg->data_len);
  }

  return true;
}

#endif /* TX_COMMIT_H */
