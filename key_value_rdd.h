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
class KeyMultiValuesRdd: public Rdd {

 public:
  KeyMultiValuesRdd() { }
  KeyMultiValuesRdd(const vector<pair<K, V>> &kvs) {
    for (auto kv : kvs) {
      kmvs_[kv.first].push_back(kv.second);
    }
  }

  KeyMultiValuesRdd(const unordered_map<K, vector<V>> &kmvs) : kmvs_(kmvs) { }

  void PrintPairs() {
    for (auto kv : kmvs_) {
      cout << "(" << kv.first << ", {";
      for (auto v : kv.second) {
        cout << v << ", ";
      }
      cout << "})" << endl;
    }
  }

  unique_ptr<KeyMultiValuesRdd> Reduce(Reducer<K, V> &reducer, hash<K> hash_fn) {
    MPI::COMM_WORLD.Barrier();
    Shuffle(hash_fn);

    vector<pair<K, V>> new_kvs;
    for (auto kmv : kmvs_) {
      new_kvs.push_back(reducer.Reduce(kmv.first, kmv.second));
    }

    return unique_ptr<KeyMultiValuesRdd<K, V>>(new KeyMultiValuesRdd<K, V>(new_kvs));
  }

 private:
  unordered_map<K, vector<V>> kmvs_;

  void Shuffle(hash<K> hash_fn) {
    vector<msgpack::sbuffer> bufs(n_workers_);
    for (auto kv : kmvs_) {
      int dest = hash_fn(kv.first) % n_workers_;
      msgpack::pack(&bufs[dest], kv);
    }

    vector<int> scounts(n_workers_);
    vector<int> sdispls(n_workers_);
    int disp = 0;
    for (int i = 0; i < n_workers_; ++i) {
      int count = bufs[i].size();
      scounts[i] = count;
      sdispls[i] = disp;
      disp += count;
      if (i > 0) {
        bufs[0].write(bufs[i].data(), count);
      }
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

    msgpack::unpacker upc;
    upc.reserve_buffer(accumulate(rcounts.begin(), rcounts.end(), 0));
    MPI::COMM_WORLD.Alltoallv(bufs[0].data(), scounts.data(), sdispls.data(), MPI::CHAR,
                              upc.buffer(), rcounts.data(), rdispls.data(), MPI::CHAR);
    upc.buffer_consumed(accumulate(rcounts.begin(), rcounts.end(), 0));

    kmvs_.clear();
    for (auto &buf : bufs) {
      free(buf.release());
    }

    msgpack::unpacked result;
    while (upc.next(&result)) {
      pair<K, vector<V>> received;
      result.get().convert(&received);
      copy(received.second.begin(), received.second.end(), back_inserter(kmvs_[received.first]));
    }
  }
};

#endif //RDD_MAPREDUCE_KEYVALUERDD_H
