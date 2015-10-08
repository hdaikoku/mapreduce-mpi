//
// Created by Harunobu Daikoku on 2015/09/02.
//

#ifndef RDD_MAPREDUCE_RDD_H
#define RDD_MAPREDUCE_RDD_H

#include <memory>
#include <string>
#include <mpi.h>

using namespace std;

class Rdd {
 public:
  Rdd();
  virtual ~Rdd() {
    if (!MPI::Is_finalized()) {
      MPI::Finalize();
    }
  }

  virtual void SplitFile(const char *filename);

 protected:
  int n_workers_;
  int mpi_my_rank_;
  MPI::Offset start_;
  MPI::Offset chunk_size_;
  unique_ptr<char[]> chunk_;
};


#endif //RDD_MAPREDUCE_RDD_H
