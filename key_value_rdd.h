//
// Created by Harunobu Daikoku on 2015/10/02.
//

#ifndef RDD_MAPREDUCE_KEYVALUERDD_H
#define RDD_MAPREDUCE_KEYVALUERDD_H

#include <vector>
#include <numeric>
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
        new_kvs.push_back(reducer.Reduce(prev, values));
        values.clear();
        prev = kv.first;
      }
      values.push_back(kv.second);
    }
    if (values.size() > 0) {
      new_kvs.push_back(reducer.Reduce(prev, values));
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

    //  pack the key-values
    msgpack::sbuffer sbuf;
    vector<int> scounts(n_workers_);
    vector<int> sdispls(n_workers_);
    int disp = 0;
    for (int i = 0; i < n_workers_; ++i) {
      msgpack::pack(sbuf, bufs[i]);
      int count = sbuf.size() - disp;
      scounts[i] = count;
      sdispls[i] = disp;
      disp += count;
    }

    vector<int> rcounts(n_workers_);
    vector<int> rdispls(n_workers_);

    // send & receive the sizes of the packed key-values
    MPI::COMM_WORLD.Alltoall(scounts.data(), 1, MPI::INT, rcounts.data(), 1, MPI::INT);
    disp = 0;
    for (int i = 0; i < n_workers_; ++i) {
      // calculate the displacements
      rdispls[i] = disp;
      disp += rcounts[i];
    }

    unique_ptr<char[]> rbuf(new char[accumulate(rcounts.begin(), rcounts.end(), 0)]);
    MPI::COMM_WORLD.Alltoallv(sbuf.data(), scounts.data(), sdispls.data(), MPI::CHAR,
                              rbuf.get(), rcounts.data(), rdispls.data(), MPI::CHAR);

    kvs_.clear();
    free(sbuf.release());

    for (int i = 0; i < n_workers_; ++i) {
      msgpack::unpacked result;
      msgpack::unpack(result, &rbuf[rdispls[i]], rcounts[i]);

      vector<pair<K, V>> received;
      result.get().convert(&received);
      kvs_.insert(kvs_.begin(), received.begin(), received.end());
    }
  }
};

#endif //RDD_MAPREDUCE_KEYVALUERDD_H
