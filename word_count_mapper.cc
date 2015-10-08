//
// Created by Harunobu Daikoku on 2015/09/18.
//

#include <sstream>
#include "word_count_mapper.h"

void WordCountMapper::Map(vector<pair<string, int>> &key_values, string &iv) {
  string word;
  istringstream iss(iv);

  while (iss >> word) {
    key_values.push_back(make_pair(word, 1));
  }
}
