FROM bitnami/spark:latest

USER root

WORKDIR /opt/spark

RUN pip install --upgrade pip

# Install OpenJDK (or another JDK of your choice) and set JAVA_HOMd

RUN { \
		echo '#!/bin/sh'; \
		echo 'set -e'; \
		echo; \
		echo 'dirname "$(dirname "$(readlink -f "$(which javac || which java)")")"'; \
	} > /usr/local/bin/docker-java-home \
	&& chmod +x /usr/local/bin/docker-java-home

ENV JAVA_HOME /usr/lib/jvm/java-9-openjdk-amd64

ENV JAVA_VERSION 9~b142
ENV JAVA_DEBIAN_VERSION 9~b142-1

COPY . .

COPY  requirements.txt .
RUN pip install -r requirements.txt

CMD ["python", "driver.py"]