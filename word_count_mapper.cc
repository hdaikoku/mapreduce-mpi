//
// Created by Harunobu Daikoku on 2015/09/18.
//

#include <sstream>
#include "word_count_mapper.h"

void WordCountMapper::Map(unordered_map<string, vector<int>> &key_values, string &iv) {
  string word;
  istringstream iss(iv);

  while (iss >> word) {
    if (key_values.find(word) == key_values.end()) {
      key_values[word].push_back(0);
    }
    key_values[word][0] += 1;
  }
}
