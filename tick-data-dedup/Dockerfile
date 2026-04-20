FROM eclipse-temurin:21-jre-alpine

LABEL maintainer="mio@qubic.org"
LABEL description="Tick Data Deduplication Service"

# needed for rocksdb
RUN apk add --no-cache libstdc++

# Create group and user with fixed IDs
ARG APP_UID=1000
ARG APP_GID=1000
RUN addgroup -g ${APP_GID} -S appgroup \
 && adduser -S -D -H -u ${APP_UID} -G appgroup appuser

WORKDIR /app

# Use a wildcard to avoid hardcoding the version
COPY target/tick-data-dedup-*.jar app.jar

# Create state directory
RUN mkdir -p /var/kafka-streams && \
    chown -R appuser:appgroup /var/kafka-streams /app

USER appuser

# JVM options
ENV JAVA_OPTS="-Xms1g -Xmx2g"

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]