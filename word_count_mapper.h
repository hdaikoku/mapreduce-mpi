//
// Created by Harunobu Daikoku on 2015/09/18.
//

#ifndef RDD_MAPREDUCE_WORDCOUNTMAPPER_H
#define RDD_MAPREDUCE_WORDCOUNTMAPPER_H

#include "mapper.h"
#include <string>

class WordCountMapper: public Mapper<string, int, string &> {

 public:
  virtual void Map(vector<pair<string, int>> &key_values, string &iv) override;
};


#endif //RDD_MAPREDUCE_WORDCOUNTMAPPER_H
