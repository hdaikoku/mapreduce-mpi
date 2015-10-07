//
// Created by Harunobu Daikoku on 2015/10/02.
//

#ifndef RDD_MAPREDUCE_KEYVALUERDD_H
#define RDD_MAPREDUCE_KEYVALUERDD_H

#include <vector>
#include "rdd.h"
#include "reducer.h"
#include "msgpack.hpp"

template<typename K, typename V>
class KeyValueRdd: public Rdd {

 public:
  KeyValueRdd() { }
  KeyValueRdd(const vector<pair<K, V>> &kvs) : kvs_(kvs) { }

  void PrintPairs() {
    for (auto kv : kvs_) {
      cout << "(" << kv.first << ", " << kv.second << ")" << endl;
    }
  }

  unique_ptr<KeyValueRdd> Reduce(Reducer<K, V> &reducer, hash<K> hash_fn) {
    MPI::COMM_WORLD.Barrier();
    Shuffle(hash_fn);
    MPI::COMM_WORLD.Barrier();

    sort(kvs_.begin(), kvs_.end());

    auto prev = kvs_[0].first;
    vector<V> values;
    vector<pair<K, V>> new_kvs;
    for (auto kv : kvs_) {
      if (prev != kv.first) {
        new_kvs.push_back(reducer.reduce(prev, values));
        values.clear();
        prev = kv.first;
      }
      values.push_back(kv.second);
    }

    return unique_ptr<KeyValueRdd<K, V>>(new KeyValueRdd<K, V>(new_kvs));
  }

 private:
  vector<pair<K, V>> kvs_;

  void Shuffle(hash<K> hash_fn) {
    vector<vector<pair<K, V>>> bufs(n_workers_);
    for (auto kv : kvs_) {
      int dest = hash_fn(kv.first) % n_workers_;
      bufs[dest].push_back(kv);
    }

    // sending out
    for (int i = 0; i < n_workers_; ++i) {
      if (i == mpi_my_rank_) {
        continue;
      }

      msgpack::sbuffer sbuf;
      msgpack::pack(sbuf, bufs[i]);

      cout << mpi_my_rank_ << ": sending " << sbuf.size() << " to " << i << endl;

      MPI::COMM_WORLD.Send(sbuf.data(), sbuf.size(), MPI::CHAR, i, 0);
    }

    kvs_.clear();
    kvs_.insert(kvs_.begin(), bufs[mpi_my_rank_].begin(), bufs[mpi_my_rank_].end());

    for (int i = 0; i < n_workers_; ++i) {
      if (i == mpi_my_rank_) {
        continue;
      }

      MPI::Status status;

      // probe the incoming message from proc. i
      MPI::COMM_WORLD.Probe(i, 0, status);
      // get the length of the receiving message
      int count = status.Get_count(MPI::CHAR);
      msgpack::sbuffer sbuf(count);

      MPI::COMM_WORLD.Recv(sbuf.data(), count, MPI::CHAR, i, 0);

      msgpack::unpacked result;
      msgpack::unpack(result, sbuf.data(), count);

      vector<pair<K, V>> received;
      result.get().convert(&received);
      kvs_.insert(kvs_.begin(), received.begin(), received.end());
    }
  }
};

#endif //RDD_MAPREDUCE_KEYVALUERDD_H
