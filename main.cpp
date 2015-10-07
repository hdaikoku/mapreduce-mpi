#include <iostream>
#include "text_rdd.h"
#include "word_count_mapper.h"
#include "word_count_reducer.h"

using namespace std;

int main(int argc, char **argv) {
  WordCountMapper wcm;
  WordCountReducer wcr;

  auto text_rdd = TextRdd::FromTextFile("/Users/HDaikoku/Desktop/word_count.txt");
  auto key_value_rdd = text_rdd->Map(wcm);
  auto new_kvs = key_value_rdd->Reduce(wcr, hash<string>());
  new_kvs->PrintPairs();

  return 0;
}