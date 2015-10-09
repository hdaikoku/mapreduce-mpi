#include <iostream>
#include "text_rdd.h"
#include "word_count_mapper.h"
#include "word_count_reducer.h"

using namespace std;

int main(int argc, char **argv) {
  WordCountMapper wcm;
  WordCountReducer wcr;

  if (argc != 2) {
    cerr << "Usage: " << argv[0] << " [text_file]" << endl;
    return 1;
  }

  auto text_rdd = TextRdd::FromTextFile(argv[1]);
  double begin = MPI::Wtime();
  auto key_value_rdd = text_rdd->Map(wcm);
  auto new_kvs = key_value_rdd->Reduce(wcr, hash<string>());
  double end = MPI::Wtime();

  new_kvs->PrintPairs();
  cout << "Total Map/Reduce time: " << end - begin << " seconds." << endl;

  return 0;
}