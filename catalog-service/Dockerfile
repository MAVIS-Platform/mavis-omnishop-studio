FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /workspace
COPY pom.xml ./
COPY api-gateway/pom.xml ./api-gateway/
COPY catalog-service/pom.xml ./catalog-service/
COPY order-service/pom.xml ./order-service/
COPY notification-worker/pom.xml ./notification-worker/
COPY catalog-service/src ./catalog-service/src
RUN mvn -pl catalog-service -am -DskipTests package

FROM eclipse-temurin:21-jre-jammy
WORKDIR /app
COPY --from=build /workspace/catalog-service/target/catalog-service-1.0.0.jar /app/app.jar
USER 1000:1000
EXPOSE 8080
HEALTHCHECK --interval=5s --timeout=3s --retries=12 CMD curl -fsS http://localhost:8080/health || exit 1
ENTRYPOINT ["java", "-XX:+UseContainerSupport", "-XX:MaxRAMPercentage=65.0", "-XX:TieredStopAtLevel=1", "-Djava.io.tmpdir=/tmp", "-jar", "/app/app.jar"]
