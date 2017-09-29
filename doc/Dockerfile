FROM python:2.7

WORKDIR /usr/src/code

RUN pip install sphinx sphinx_rtd_theme

RUN apt-get update \
    && apt-get install -y \
        ant \
        default-jdk

CMD ant realclean \
    && ant gen-doc \
    && echo "The locally built documentation can be found here:\n\n    open build/html/index.html\n\n"
