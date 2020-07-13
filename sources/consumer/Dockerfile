FROM openjdk:8-jdk-slim

ARG USER_HOME_DIR="/root"

# Updating the image
RUN apt-get update

# Installing the dependencies 
RUN apt-get install -y curl \
                       bash \
                       ca-certificates \
                       dirmngr \
                       gosu \
                       gnupg \
                       python \
                       procps

# Verifying that gosu binary works
RUN gosu nobody true

# Installing Maven (https://github.com/carlossg/docker-maven/blob/master/openjdk-8-slim/Dockerfile)
ARG MAVEN_VERSION=3.6.3
ARG MAVEN_SHA=c35a1803a6e70a126e80b2b3ae33eed961f83ed74d18fcd16909b2d44d7dada3203f1ffe726c17ef8dcca2dcaa9fca676987befeadc9b9f759967a8cb77181c0
ARG MAVEN_URL=https://apache.osuosl.org/maven/maven-3/${MAVEN_VERSION}/binaries

RUN mkdir -p /usr/share/maven /usr/share/maven/ref \
  && curl -fsSL -o /tmp/apache-maven.tar.gz ${MAVEN_URL}/apache-maven-${MAVEN_VERSION}-bin.tar.gz \
  && echo "${MAVEN_SHA}  /tmp/apache-maven.tar.gz" | sha512sum -c - \
  && tar -xzf /tmp/apache-maven.tar.gz -C /usr/share/maven --strip-components=1 \
  && rm -f /tmp/apache-maven.tar.gz \
  && ln -s /usr/share/maven/bin/mvn /usr/bin/mvn

ENV MAVEN_HOME /usr/share/maven
ENV MAVEN_CONFIG "$USER_HOME_DIR/.m2"

# Installing Storm (https://github.com/31z4/storm-docker/blob/4c2804fa4365f62c3b6009d3496661542e4d446a/2.2.0/Dockerfile)
ARG STORM_VERSION=2.2.0
ARG STORM_GPG_KEY=79B03D059E628478FC9F1D8B152CAD0C46E87B61
ARG STORM_URL=https://www.apache.org/dist/storm/apache-storm-${STORM_VERSION}

RUN mkdir -p /usr/share/storm /usr/share/storm/ref \
  && curl -fsSL -o /tmp/apache-storm.tar.gz ${STORM_URL}/apache-storm-${STORM_VERSION}.tar.gz \
  && tar -xzf /tmp/apache-storm.tar.gz -C /usr/share/storm --strip-components=1 \
  && rm -f /tmp/apache-storm.tar.gz

ENV PATH $PATH:/usr/share/storm/bin

# Installing watcher
WORKDIR /usr/src/app

COPY watcher/ ./watcher

WORKDIR /usr/src/app/watcher

# Building the topology 
RUN mvn clean package

CMD ["storm", "jar", "target/watcher-1.0-SNAPSHOT.jar", "com.btc.Main"]