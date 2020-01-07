# Dockerfile for building the Cassandra documentation.
# If wanting to regenerate the documentation from scratch,
# run `ant realclean` from the root directory of this project.

FROM python:2.7

WORKDIR /usr/src/code

RUN pip install --no-cache-dir sphinx sphinx_rtd_theme

RUN apt-get update && apt-get install -y software-properties-common

RUN wget -qO - https://adoptopenjdk.jfrog.io/adoptopenjdk/api/gpg/key/public | apt-key add - \
    && add-apt-repository --yes https://adoptopenjdk.jfrog.io/adoptopenjdk/deb/ \
    && apt-get update \
    && apt-get install -y adoptopenjdk-8-hotspot ant


RUN apt-get clean

CMD CASSANDRA_USE_JDK11=true ant gen-doc \
    && echo "The locally built documentation can be found here:\n\n    build/html/index.html\n\n"
