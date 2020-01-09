FROM openjdk:8-jre-slim
#Install curl for health check
RUN apt-get update && apt-get install -y --no-install-recommends curl
ADD target/transitdata-hfp-downgrader.jar /usr/app/transitdata-hfp-downgrader.jar
ENTRYPOINT ["java", "-jar", "/usr/app/transitdata-hfp-downgrader.jar"]
