FROM eclipse-temurin:21-jre
WORKDIR /app
COPY build/libs/processing-service-streams-*.jar app.jar
ENV JAVA_OPTS=""
ENTRYPOINT ["/bin/sh","-c","java $JAVA_OPTS -jar /app/app.jar"]
