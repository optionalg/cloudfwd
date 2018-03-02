# Base image to get a container ready to run cloudfwd tests
#
# to build run:
# docker build . -f Dockerfile.docker-splunk-6.6.3-sun-java8
FROM mlaccetti/docker-oracle-java8-ubuntu-16.04

# make the "en_US.UTF-8" locale so splunk will be utf-8 enabled by default
RUN apt-get update && apt-get install -y locales \
    && localedef -i en_US -c -f UTF-8 -A /usr/share/locale/locale.alias en_US.UTF-8
ENV LANG en_US.utf8

# install very base dependencies
RUN apt-get install -y --no-install-recommends \
    bzip2 \
    unzip \
    xz-utils \
    procps \
    pstack \
    wget \
    sudo \
    libgssapi-krb5-2 \
    git \
    vim \
    ca-certificates \
    software-properties-common \
    python-software-properties \
    && apt-get --purge remove openjdk*

# Define commonly used JAVA_HOME variable
ENV JAVA_HOME /usr/lib/jvm/java-8-oracle

# Install maven depending on Java
RUN apt-get update && apt-get install -y --no-install-recommends maven