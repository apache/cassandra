# Dockerfile for building the Cassandra documentation.
# If wanting to regenerate the documentation from scratch,
# run `ant realclean` from the root directory of this project.

FROM python:2.7

WORKDIR /usr/src/code

RUN pip install sphinx sphinx_rtd_theme

RUN apt-get update \
    && apt-get install -y \
        ant \
        default-jdk

CMD ant gen-doc \
    && echo "The locally built documentation can be found here:\n\n    build/html/index.html\n\n"
