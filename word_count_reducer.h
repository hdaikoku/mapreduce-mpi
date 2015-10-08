//
// Created by Harunobu Daikoku on 2015/10/05.
//

#ifndef RDD_MAPREDUCE_WORD_COUNT_REDUCER_H
#define RDD_MAPREDUCE_WORD_COUNT_REDUCER_H

#include "reducer.h"

class WordCountReducer: public Reducer<string, int> {

 public:
  virtual pair<string, int> Reduce(string k, const vector<int> &vector1);
};


#endif //RDD_MAPREDUCE_WORD_COUNT_REDUCER_H
