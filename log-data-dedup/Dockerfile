FROM eclipse-temurin:17-jre-alpine

LABEL maintainer="mio@qubic.org"
LABEL description="Event Log Deduplication Service"

WORKDIR /app

# Copy JAR
COPY target/dedup-app-1.0.0.jar app.jar

# Create state directory
RUN mkdir -p /var/kafka-streams && \
    addgroup -S appgroup && \
    adduser -S appuser -G appgroup && \
    chown -R appuser:appgroup /var/kafka-streams /app

USER appuser

# JVM options
ENV JAVA_OPTS="-Xms2g -Xmx4g -XX:+UseG1GC -XX:MaxGCPauseMillis=200"

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=60s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8080/actuator/health || exit 1

EXPOSE 8080

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]