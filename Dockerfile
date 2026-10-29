# Build Cruise Control in its own stage
FROM amazoncorretto:21-alpine-jdk AS build
WORKDIR /workspace
COPY . .
RUN ./gradlew clean jar copyDependantLibs --warning-mode all

# Fetch the jars, configs, and the startup script and run Cruise Control
FROM amazoncorretto:21-alpine AS runtime
RUN apk add --no-cache bash
WORKDIR /cc
COPY --from=build /workspace/cruise-control/build/ /cc/cruise-control/build/
COPY --from=build /workspace/config/ /cc/config/
COPY --from=build /workspace/kafka-cruise-control-start.sh /cc/
RUN chmod +x kafka-cruise-control-start.sh
EXPOSE 9090
CMD ["./kafka-cruise-control-start.sh", "config/cruisecontrol.properties", "9090"]
