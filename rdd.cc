//
// Created by Harunobu Daikoku on 2015/09/02.
//

#include "rdd.h"

Rdd::Rdd() {
  if (!MPI::Is_initialized()) {
    MPI::Init();
  }
  mpi_my_rank_ = MPI::COMM_WORLD.Get_rank();
  n_workers_ = MPI::COMM_WORLD.Get_size();
}

//TODO: error handling
void Rdd::SplitFile(const char *filename) {
  MPI::File file;
  MPI::Offset file_size;

  file = MPI::File::Open(MPI::COMM_WORLD, filename, MPI::MODE_RDONLY, MPI::INFO_NULL);
  if (file == MPI::FILE_NULL) {
    cerr << "ERROR: MPI::File::Open" << endl;
    return;
  }

  file_size = file.Get_size();
  // ignore EOF
  file_size -= 1;

  chunk_size_ = file_size / n_workers_;
  start_ = mpi_my_rank_ * chunk_size_;
  if (mpi_my_rank_ == n_workers_ - 1) {
    chunk_size_ = file_size - start_;
  }
  if (mpi_my_rank_ != 0) {
    chunk_size_ += 1;
  }

  // assign the buffer
  chunk_.reset(new char[chunk_size_ + 1]);
  file.Read_at_all(mpi_my_rank_ == 0 ? start_ : start_ - 1,
                   chunk_.get(),
                   mpi_my_rank_ == 0 ? chunk_size_ : chunk_size_ + 1,
                   MPI::CHAR);

  file.Close();
}
