#ifndef SDS_MAPPING_H
#define SDS_MAPPING_H

inline bool should_connect(int global_client_id, int global_worker_id,
                           int nr_workers) {
  return global_worker_id == global_client_id % nr_workers;
}

inline int get_worker_id(int global_client_id, int nr_workers) {
  return global_client_id % nr_workers;
}

#endif  // SDS_MAPPING_H
