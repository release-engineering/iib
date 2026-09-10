# Based from https://github.com/rmohr/docker-activemq
# Base layer pinned by digest to the linux/amd64 manifest of ubi9/ubi:latest
# (9.8-1788939089 as of this pin). Note the `dnf update -y` below still pulls current
# packages, so the full image is not bit-for-bit reproducible.
# Keep this digest in lockstep with the other Dockerfiles when bumping.
FROM registry.access.redhat.com/ubi9/ubi@sha256:4406835b776f05688a823ff532203587b9238510817645c9f37e0fdac1743b8c

ENV ACTIVEMQ_VERSION 5.15.12
ENV ACTIVEMQ_HOME /opt/activemq

# keeping container image updated (security updates)
RUN  dnf install java-1.8.0-openjdk maven -y && dnf update -y && dnf clean all

RUN curl "https://archive.apache.org/dist/activemq/$ACTIVEMQ_VERSION/apache-activemq-$ACTIVEMQ_VERSION-bin.tar.gz" -o activemq-bin.tar.gz
RUN tar xzf activemq-bin.tar.gz -C  /opt && \
    ln -s /opt/apache-activemq-$ACTIVEMQ_VERSION $ACTIVEMQ_HOME
COPY ./docker/message_broker/activemq.xml $ACTIVEMQ_HOME/conf/activemq.xml
RUN useradd -r -M -d $ACTIVEMQ_HOME activemq && \
    chown -R activemq:activemq /opt/$ACTIVEMQ && \
    chown -h activemq:activemq $ACTIVEMQ_HOME
USER activemq

WORKDIR $ACTIVEMQ_HOME
# Expose the AMQP, AMQPS, and web console ports
EXPOSE 5761 5762 8161

CMD ["/opt/activemq/bin/activemq", "console"]
