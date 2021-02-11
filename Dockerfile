ARG UBUNTU_TAG=focal
FROM ubuntu:${UBUNTU_TAG} as ubuntu
ENV DEBIAN_FRONTEND noninteractive
ARG OPAE_VERSION=2.0.1-2
ENV OPAE_VERSION ${OPAE_VERSION}

FROM ubuntu as opae
RUN apt-get update && \
    apt-get install -y \
    g++ \
    make\
    cmake\
    git\
    uuid-dev\
    libjson-c-dev\
    libhwloc-dev \
    python3-dev \
    libtbb-dev \
    lsb-release && \
    git clone --single-branch --branch release/${OPAE_VERSION} https://github.com/OPAE/opae-sdk.git /opae-sdk && \
    cmake \
    -DCMAKE_BUILD_TYPE=Release \
    -DCPACK_GENERATOR=DEB /opae-sdk \
    -DOPAE_BUILD_LIBOPAE_PY=Off \
    -DOPAE_BUILD_LIBOPAEVFIO=Off \
    -DOPAE_BUILD_PLUGIN_VFIO=Off \
    -DOPAE_BUILD_LIBOPAEUIO=Off \
    -DOPAE_BUILD_EXTRA_TOOLS=Off && \
    make -j package

FROM ubuntu as deps
ARG ARROW_VERSION=3.0.0
ARG PULSAR_VERSION=2.7.0
ENV PULSAR_VERSION ${PULSAR_VERSION}
ARG FLETCHER_VERSION=0.0.19
ARG FLETCHER_OPAE_VERSION=0.2.1
COPY --from=opae /opae-${OPAE_VERSION}.x86_64-libs.deb opae-${OPAE_VERSION}.x86_64-libs.deb
RUN apt-get update && \
    # arrow
    apt-get install -y curl wget lsb-release gnupg && \
    wget https://apache.bintray.com/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-archive-keyring-latest-$(lsb_release --codename --short).deb && \
    dpkg -i apache-arrow-archive-keyring-latest-$(lsb_release --codename --short).deb && \
    apt-get update && \
    apt-get install -y libarrow-dev=$ARROW_VERSION-1 && \
    # pulsar
    curl -L -O https://downloads.apache.org/pulsar/pulsar-${PULSAR_VERSION}/DEB/apache-pulsar-client.deb && \
    dpkg -i apache-pulsar-client.deb && \
    # opae
    apt-get install -y uuid-dev libjson-c-dev && \
    dpkg -i /opae-${OPAE_VERSION}.x86_64-libs.deb && \
    # fletcher
    wget https://github.com/abs-tudelft/fletcher/releases/download/${FLETCHER_VERSION}/fletcher_${FLETCHER_VERSION}-ubuntu$(lsb_release --release --short)_amd64.deb && \
    dpkg -i fletcher_${FLETCHER_VERSION}-ubuntu$(lsb_release --release --short)_amd64.deb && \
    # fletcher-opae
    wget https://github.com/teratide/fletcher-opae/releases/download/${FLETCHER_OPAE_VERSION}/fletcher_opae_${FLETCHER_OPAE_VERSION}-ubuntu$(lsb_release --release --short)_amd64.deb && \
    dpkg -i fletcher_opae_${FLETCHER_OPAE_VERSION}-ubuntu$(lsb_release --release --short)_amd64.deb && \
    # clean-up
    apt-get remove -y --purge curl wget lsb-release gnupg apache-arrow-archive-keyring cmake g++ make git && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/* *.deb

FROM deps as build
RUN apt-get update && \
    apt-get install -y g++ make cmake git curl && \
    curl -L -O https://downloads.apache.org/pulsar/pulsar-${PULSAR_VERSION}/DEB/apache-pulsar-client-dev.deb && \
    dpkg -i apache-pulsar-client-dev.deb
ADD . /src
WORKDIR /release
RUN cmake -DCMAKE_BUILD_TYPE=Release /src && \
    make -j

FROM deps
COPY --from=build /release/bolson /bolson
ENTRYPOINT [ "/bolson" ]
CMD ["--help"]
