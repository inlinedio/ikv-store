FROM eclipse-temurin:17-jdk-jammy

COPY build/libs/ikv-cloud.jar /ikv-cloud/ikv-cloud.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/ikv-cloud/ikv-cloud.jar"]

#RUN mkdir -p /inline-gateway
#COPY src /inline-gateway
#WORKDIR /inline-gateway
#RUN apt-get -y update && \
    #apt-get install -y --no-install-recommends wget unzip
#RUN wget https://services.gradle.org/distributions/gradle-8.0.2-bin.zip
#RUN mkdir /opt/gradle
#RUN unzip -d /opt/gradle gradle-8.0.2-bin.zip
#ENV PATH="/opt/gradle/gradle-8.0.2/bin:${PATH}"
#EXPOSE 8080
#RUN gradle build
#CMD ["gradle", "execute"]
