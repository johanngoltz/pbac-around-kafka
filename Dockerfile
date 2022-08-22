FROM sapmachine:17
RUN mkdir /opt/pbac-around-kafka && mkdir /opt/pbac-around-kafka/config
COPY target/*-jar-with-dependencies.jar /opt/pbac-around-kafka/app.jar
COPY config/ /opt/pbac-around-kafka/config/
CMD ["java", "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005", "-jar", "/opt/pbac-around-kafka/app.jar", "/opt/pbac-around-kafka/config/server.properties"]