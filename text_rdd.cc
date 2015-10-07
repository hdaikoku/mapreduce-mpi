//
// Created by Harunobu Daikoku on 2015/09/14.
//

#include <sstream>
#include "text_rdd.h"

unique_ptr<TextRdd> TextRdd::FromTextFile(const char *filename) {
  unique_ptr<TextRdd> text_rdd(new TextRdd());
  text_rdd->SplitFile(filename);
  return text_rdd;
}

void TextRdd::SplitFile(const char *filename) {
  Rdd::SplitFile(filename);

  if (mpi_my_rank_ != 0) {
    // send the first line to the preceding proc.
    int i = 0;
    while (chunk_[i] != '\n' && i < chunk_size_) {
      // calculate the length of the first line
      i++;
    }

    MPI::COMM_WORLD.Send(chunk_.get(), i, MPI::CHAR, mpi_my_rank_ - 1, 0);
  }

  if (mpi_my_rank_ != n_workers_ - 1) {
    // receive the first line of the succeeding proc.
    MPI::Status status;

    // probe the incoming message from pc. mpi_my_rank_ + 1
    MPI::COMM_WORLD.Probe(mpi_my_rank_ + 1, 0, status);
    // get the length of the receiving line
    remote_line_length_ = status.Get_count(MPI::CHAR);

    remote_line_.reset(new char[remote_line_length_]);

    MPI::COMM_WORLD.Recv(remote_line_.get(), remote_line_length_, MPI::CHAR, mpi_my_rank_ + 1, 0);
  }
}

