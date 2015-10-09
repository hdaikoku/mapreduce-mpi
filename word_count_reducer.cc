//
// Created by Harunobu Daikoku on 2015/10/05.
//

#include <string>
#include "word_count_reducer.h"

pair<string, int> WordCountReducer::Reduce(const string &k, const vector<int> &vector1) {
  int sum = 0;
  for (auto v : vector1) {
    sum += v;
  }

  return make_pair(k, sum);
}
