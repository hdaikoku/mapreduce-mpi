//
// Created by Harunobu Daikoku on 2015/09/02.
//

#ifndef RDD_MAPREDUCE_MAPPER_H
#define RDD_MAPREDUCE_MAPPER_H

#include <vector>
#include <unordered_map>

using namespace std;

template<typename K, typename V, typename IV>
class Mapper {
 public:
  virtual void Map(unordered_map<K, vector<V>> &, IV) = 0;
};

#endif //RDD_MAPREDUCE_MAPPER_H
