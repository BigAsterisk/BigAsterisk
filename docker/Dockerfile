# Reproducible BigAsterisk environment: Spark 4.1.2 + JDK 17 + Python 3, with the
# platform jars built from source and the end-to-end notebooks.
#
#   docker build -t bigasterisk -f docker/Dockerfile .
#   docker run --rm -p 8888:8888 bigasterisk        # JupyterLab at :8888
#   docker run --rm bigasterisk validate            # execute all notebooks headlessly

# ---- build stage: compile the platform jars from source -----------------------
FROM eclipse-temurin:17-jdk-jammy AS build
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /build
RUN curl -sL https://github.com/sbt/sbt/releases/download/v1.10.7/sbt-1.10.7.tgz | tar xz
COPY build.sbt .
COPY project project
COPY modules modules
RUN ./sbt/bin/sbt -batch package \
    && mkdir -p /build/jars \
    && find /build/modules -name 'bigasterisk-*.jar' -exec cp {} /build/jars/ \; \
    && cp "$(find /root/.cache/coursier -name 'fastutil-8.5.15.jar' | head -1)" \
          /build/jars/fastutil.jar

# ---- runtime stage ------------------------------------------------------------
FROM eclipse-temurin:17-jdk-jammy
RUN apt-get update && apt-get install -y --no-install-recommends \
        python3 python3-pip procps \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf /usr/bin/python3 /usr/local/bin/python
RUN pip3 install --no-cache-dir pyspark==4.1.2 jupyterlab nbconvert

WORKDIR /opt/bigasterisk
COPY --from=build /build/jars jars
COPY python python
COPY notebooks notebooks
COPY modules/spark4/src/test/resources modules/spark4/src/test/resources
COPY examples/data examples/data
COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENV BIGASTERISK_HOME=/opt/bigasterisk \
    PYTHONPATH=/opt/bigasterisk/python

EXPOSE 8888
ENTRYPOINT ["/entrypoint.sh"]
CMD ["lab"]
