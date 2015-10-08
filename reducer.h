//
// Created by Harunobu Daikoku on 2015/09/02.
//

#ifndef RDD_MAPREDUCE_REDUCER_H
#define RDD_MAPREDUCE_REDUCER_H

#include <vector>

using namespace std;

template<typename K, typename V>
class Reducer {
 public:
  virtual pair<K, V> Reduce(K, const vector<V> &) = 0;
};

#endif //RDD_MAPREDUCE_REDUCER_H
