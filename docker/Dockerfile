FROM sequenceiq/spark:1.4.0
MAINTAINER hdaikoku <daikoku@hpcs.cs.tsukuba.ac.jp>

# prepare devtools-2 repo
RUN curl -o /etc/yum.repos.d/devtools-2.repo http://people.centos.org/tru/devtools-2/devtools-2.repo -O

# install gcc-4.9, openmpi, cmake
RUN yum -y install devtoolset-2-gcc-c++ devtoolset-2-binutils openmpi-devel cmake \
 && source /opt/rh/devtoolset-2/enable

ENV PATH $PATH:/usr/lib64/openmpi/bin
ENV LD_LIBRARY_PATH $LD_LIBRARY_PATH:/usr/lib64/openmpi/lib

RUN yum -y clean all

CMD ["scl", "enable", "devtoolset-2", "bash"]