FROM eclipse-temurin:21-jre-alpine

LABEL maintainer="mio@qubic.org"
LABEL description="Event Log Deduplication Service"

# needed for rocksdb
RUN apk add --no-cache libstdc++

# Create group and user with fixed IDs
ARG APP_UID=1000
ARG APP_GID=1000
RUN addgroup -g ${APP_GID} -S appgroup \
 && adduser -S -D -H -u ${APP_UID} -G appgroup appuser

WORKDIR /app

# Copy the built application JAR from the module's target directory
# Use a wildcard to avoid hardcoding the version
COPY target/log-data-dedup-*.jar app.jar

# Create state directory
RUN mkdir -p /var/kafka-streams && \
    chown -R appuser:appgroup /var/kafka-streams /app

USER appuser

# JVM options
ENV JAVA_OPTS="-Xms2g -Xmx4g -XX:+UseG1GC -XX:MaxGCPauseMillis=200"

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=60s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8080/actuator/health || exit 1

EXPOSE 8080

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]