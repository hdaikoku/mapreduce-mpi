//
// Created by Harunobu Daikoku on 2015/09/02.
//

#ifndef RDD_MAPREDUCE_MAPPER_H
#define RDD_MAPREDUCE_MAPPER_H

#include <vector>

using namespace std;

template<typename K, typename V, typename IV>
class Mapper {
 public:
  virtual void Map(IV, vector<pair<K, V>> &) = 0;
};

#endif //RDD_MAPREDUCE_MAPPER_H
