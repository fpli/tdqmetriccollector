# It's better to use multi-state build to reduce image size
FROM hub.tess.io/adihadoop/maven:3.8-jdk-8-slim AS builder

ENV MAVEN_OPTS="-Dmaven.wagon.http.ssl.insecure=true -Dmaven.wagon.http.ssl.allowall=true -Dmaven.wagon.http.ssl.ignore.validity.dates=true"
RUN mvn -s /workspace/maven-settings.xml clean package -f /workspace -DskipTests

# Should use scratch to reduce image size
FROM scratch
COPY --from=builder /workspace/target/*-jar-with-dependencies.jar app.jar

# Alternatively you specify a comma separated list of file glob patterns, e.g. "*.jar,*.tar,etc/*.conf"
LABEL com.ebay.adi.adlc.include="*.jar"

# Alternatively you specify a comma separated list of tags, e.g. "latest,1.0.0"
LABEL com.ebay.adi.adlc.tag="1.0.0"

# Send Email to fangpli@ebay.com and yxiao6@ebay.com
LABEL org.opencontainers.image.authors="fangpli@ebay.com,yxiao6"