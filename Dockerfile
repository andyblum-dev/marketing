# Stage 1: Build Stage
FROM amazoncorretto:21 AS builder
LABEL maintainer="Apache ActiveMQ Team"
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
WORKDIR /opt

# Define environment variables and install necessary packages
ARG ARTEMIS_VERSION=2.36.0
ENV ARTEMIS_DIST_DIR=/opt/activemq-artemis

RUN yum -y --setopt=skip_if_unavailable=1 update && \
    yum install -y --setopt=skip_if_unavailable=1 curl tar gzip && \
    curl https://archive.apache.org/dist/activemq/activemq-artemis/$ARTEMIS_VERSION/apache-artemis-$ARTEMIS_VERSION-bin.tar.gz -o apache-artemis-$ARTEMIS_VERSION-bin.tar.gz && \
    mkdir -p $ARTEMIS_DIST_DIR && \
    tar xzf apache-artemis-$ARTEMIS_VERSION-bin.tar.gz -C $ARTEMIS_DIST_DIR --strip-components=1 && \
    yum clean all && \
    rm -f apache-artemis-$ARTEMIS_VERSION-bin.tar.gz

# Stage 2: Final Stage
FROM amazoncorretto:21
LABEL maintainer="Apache ActiveMQ Team"
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
WORKDIR /opt

# Define arguments and environment variables
ARG PUBLIC_IP
ARG ARTEMIS_USER
ARG ARTEMIS_PASSWORD

ENV ARTEMIS_USER=$ARTEMIS_USER \
    ARTEMIS_PASSWORD=$ARTEMIS_PASSWORD \
    ARTEMIS_DIST_DIR=/opt/activemq-artemis \
    ANONYMOUS_LOGIN=false \
    EXTRA_ARGS="--http-host $PUBLIC_IP --no-amqp-acceptor --no-hornetq-acceptor --no-mqtt-acceptor --no-web"

RUN yum -y --setopt=skip_if_unavailable=1 update && \
    yum install -y --setopt=skip_if_unavailable=1 shadow-utils libaio net-tools && \
    groupadd -g 1001 -r artemis && \
    useradd -r -u 1001 -g artemis artemis && \
    yum clean all

# Copy ActiveMQ Artemis from the build stage
COPY --from=builder /opt/activemq-artemis $ARTEMIS_DIST_DIR

# Expose port for CORE,MQTT,AMQP,HORNETQ,STOMP,OPENWIRE
EXPOSE 61616

# Set ownership and permissions for Artemis instance directory
RUN mkdir /var/lib/artemis-instance && \
    chown -R artemis:artemis /var/lib/artemis-instance

# Copy the startup script
COPY /docker-entrypoint.sh /
RUN chmod +x /docker-entrypoint.sh

# Switch to the artemis user
USER artemis

# Expose some outstanding folders
VOLUME ["/var/lib/artemis-instance"]
WORKDIR /var/lib/artemis-instance

# Define the entrypoint and default command
ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["run"]