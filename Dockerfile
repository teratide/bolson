FROM ubuntu:focal

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update && \
    apt-get install -y \
    curl cmake g++ make git \
    ca-certificates lsb-release wget gnupg \
    uuid-dev libjson-c-dev libhwloc-dev libtbb-dev python-dev python3-pip && \
    pip3 install -Iv pyarrow==3.0.0

ARG OPAE_REF=release/2.0.0
RUN mkdir -p /opae-sdk/build && \
    curl -L https://github.com/OPAE/opae-sdk/archive/${OPAE_REF}.tar.gz | tar xz -C /opae-sdk --strip-components=1 && \
    cd /opae-sdk/build && \
    cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/usr .. && \
    make -j && \
    make install && \
    rm -rf /opae-sdk/build

ARG FLETCHER_OPAE_REF=6a06c02a766bb7e6119feb02cce0f718a8fd5416
RUN git clone -b multi_battery https://github.com/teratide/fletcher-opae.git /fletcher-opae && \
    cd /fletcher-opae && \
    cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/usr . && \
    make -j && \
    make install && \
    rm -rf /fletcher-opae

#ARG ARROW_REF=apache-arrow-3.0.0
#RUN wget https://apache.bintray.com/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-archive-keyring-latest-$(lsb_release --codename --short).deb && \
#    dpkg -i apache-arrow-archive-keyring-latest-$(lsb_release --codename --short).deb && \
#    apt-get update && \
#    apt-get install -y libarrow-dev

# Temporary work-around for Arrow support for fixed size list json parsing
RUN git clone -b json-fixed-size-list https://github.com/johanpel/arrow.git /arrow && \
    cd /arrow/cpp && \
    cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/usr -DARROW_JSON=ON -DARROW_COMPUTE=ON . && \
    make -j && \
    make install && \
    rm -rf /arrow

ARG PULSAR_VERSION=2.7.0
RUN curl -L -O https://downloads.apache.org/pulsar/pulsar-${PULSAR_VERSION}/DEB/apache-pulsar-client.deb && \
    dpkg -i apache-pulsar-client.deb && \
    curl -L -O https://downloads.apache.org/pulsar/pulsar-${PULSAR_VERSION}/DEB/apache-pulsar-client-dev.deb && \
    dpkg -i apache-pulsar-client-dev.deb

ADD . /src

WORKDIR /src
RUN cmake -DCMAKE_BUILD_TYPE=Release . && \
    make -j && \
    make install
