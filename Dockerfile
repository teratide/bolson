FROM ubuntu:focal as base
ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update && \
    apt-get install -y g++ make cmake curl ca-certificates lsb-release wget gnupg git

FROM base as arrow
ARG ARROW_VERSION=1.0.1-1
RUN apt-get install -y && \
    wget https://apache.bintray.com/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-archive-keyring-latest-$(lsb_release --codename --short).deb && \
    dpkg -i apache-arrow-archive-keyring-latest-$(lsb_release --codename --short).deb && \
    apt-get update && \
    apt-get install -y libarrow-dev=${ARROW_VERSION}

FROM arrow as pulsar
ARG PULSAR_VERSION=2.7.0
RUN curl -L -O https://downloads.apache.org/pulsar/pulsar-${PULSAR_VERSION}/DEB/apache-pulsar-client.deb && \
    dpkg -i apache-pulsar-client.deb && \
    curl -L -O https://downloads.apache.org/pulsar/pulsar-${PULSAR_VERSION}/DEB/apache-pulsar-client-dev.deb && \
    dpkg -i apache-pulsar-client-dev.deb

FROM pulsar as bolson
ADD . /bolson
WORKDIR /bolson/release
# RUN cmake -DCMAKE_BUILD_TYPE=Release .. && \
RUN cmake -DCMAKE_BUILD_TYPE=Debug .. && \
    make -j && \
    make install
ADD battery.as /bolson/release
ENV LD_LIBRARY_PATH=/usr/local/lib/
ENTRYPOINT [ "bolson" ]

FROM arrow as illex
ARG ILLEX_REF=master
WORKDIR /illex/release
RUN curl -L https://github.com/teratide/illex/archive/${ILLEX_REF}.tar.gz | tar xz --strip-components=1 -C /illex && \
    cmake -DCMAKE_BUILD_TYPE=Release .. && \
    make -j && \
    make install
ADD battery.as /illex/release
ENTRYPOINT [ "illex" ]
