# Build stage
FROM maven:3.6.3-jdk-8-slim AS builder

MAINTAINER Taveesh Sharma <shrtav001@myuct.ac.za>

RUN mkdir -p /build
WORKDIR /build
COPY pom.xml /build
# Download all required dependencies into one layer
RUN mvn -B dependency:resolve dependency:resolve-plugins
# Copy source code
COPY src /build/src
RUN mvn package -DskipTests

FROM ubuntu:xenial
MAINTAINER shrtav001@myuct.ac.za

RUN apt-get update && apt-get install -y \
    net-tools \
    iputils-ping \
    iproute

# This is in accordance to : https://www.digitalocean.com/community/tutorials/how-to-install-java-with-apt-get-on-ubuntu-16-04
RUN apt-get update && \
	apt-get install -y openjdk-8-jdk && \
	apt-get install -y ant && \
	apt-get clean && \
	rm -rf /var/lib/apt/lists/* && \
	rm -rf /var/cache/oracle-jdk8-installer;

# Fix certificate issues, found as of
# https://bugs.launchpad.net/ubuntu/+source/ca-certificates-java/+bug/983302
RUN apt-get update && \
	apt-get install -y ca-certificates-java && \
	apt-get clean && \
	update-ca-certificates -f && \
	rm -rf /var/lib/apt/lists/* && \
	rm -rf /var/cache/oracle-jdk8-installer;

# Setup JAVA_HOME, this is useful for docker commandline
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/
RUN export JAVA_HOME

COPY --from=builder /build/target/*.jar app.jar

EXPOSE 8080

ENTRYPOINT ["java","-jar","app.jar"]
CMD ["tail", "-f", "/dev/null"]