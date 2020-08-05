FROM ubuntu:focal

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update && \
    apt-get install -y g++ make cmake curl ca-certificates lsb-release wget gnupg git && \
    wget https://apache.bintray.com/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-archive-keyring-latest-$(lsb_release --codename --short).deb && \
    dpkg -i apache-arrow-archive-keyring-latest-$(lsb_release --codename --short).deb && \
    apt-get update && \
    apt-get install -y libarrow-dev && \
    curl -L -O https://downloads.apache.org/pulsar/pulsar-2.6.0/DEB/apache-pulsar-client.deb && \
    dpkg -i apache-pulsar-client.deb && \
    curl -L -O https://downloads.apache.org/pulsar/pulsar-2.6.0/DEB/apache-pulsar-client-dev.deb && \
    dpkg -i apache-pulsar-client-dev.deb

ADD . /src

WORKDIR /src/release
RUN cmake -DCMAKE_BUILD_TYPE=Release /src && \
    make -j
