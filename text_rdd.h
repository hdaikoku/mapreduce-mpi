//
// Created by Harunobu Daikoku on 2015/09/14.
//

#ifndef RDD_MAPREDUCE_TEXT_RDD_H
#define RDD_MAPREDUCE_TEXT_RDD_H

#include <sstream>
#include "rdd.h"
#include "key_value_rdd.h"
#include "mapper.h"

class TextRdd: public Rdd {
 public:
  static unique_ptr<TextRdd> FromTextFile(const char *filename);

  template<typename K, typename V, typename IV>
  unique_ptr<KeyValueRdd<K, V>> Map(Mapper<K, V, IV> &mapper) {
    vector<pair<string, int>> key_values;
    string line;
    string str(chunk_.get(), chunk_size_);

    if (remote_line_) {
      // append the first line of the succeeding proc.
      str.append(remote_line_.get());
    }

    istringstream str_stream(str);
    if (mpi_my_rank_ != 0) {
      // discard the first line.
      getline(str_stream, line);
    }

    while (getline(str_stream, line)) {
      mapper.Map(key_values, line);
    }

    return unique_ptr<KeyValueRdd<K, V>>(new KeyValueRdd<K, V>(key_values));
  }

 private:
  int remote_line_length_;
  unique_ptr<char[]> remote_line_;

  virtual void SplitFile(const char *filename) override;
};


#endif //RDD_MAPREDUCE_TEXT_RDD_H
