# Dockerfile for running Cassandra unit tests
# Uses official Ubuntu image as base
FROM ubuntu:24.04

# Prevent interactive prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

# Install OpenJDK 11 and required build tools
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    ant \
    ant-optional \
    git \
    python3 \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /cassandra

# Set environment variables for Cassandra build
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV ANT_HOME=/usr/share/ant
ENV PATH="${ANT_HOME}/bin:${PATH}"

# Default command - run the full test suite
CMD [".build/run-tests.sh", "-a", "test"]
