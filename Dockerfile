# The distdb binary in a container, with the network tools the chaos suite drives (iptables, tc).
# Build the distribution first:  ./gradlew :distdb-server:installDist
FROM eclipse-temurin:25-jre-noble
RUN apt-get update \
 && apt-get install -y --no-install-recommends iptables iproute2 \
 && rm -rf /var/lib/apt/lists/*
COPY distdb-server/build/install/distdb /opt/distdb
ENV PATH="/opt/distdb/bin:${PATH}"
WORKDIR /data
ENTRYPOINT ["distdb"]
