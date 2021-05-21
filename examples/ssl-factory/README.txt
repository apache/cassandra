Cassandra Custom SslContextFactory Example:
==========================================

Example-1: Custom SslContextFactory implementation based on Kubernetes secrets
------------------------------------------------------------------------------
For the documentation please refer to the javadocs for the K8SecretsSslContextFactory.java.


Installation:
============
Step 1: Build the Cassandra classes locally

change directory to <cassandra_src_dir>
run "ant build"

Step 2: Run tests for the security examples

change directory to <cassandra_src_dir>/examples/ssl-factory
run "ant test"
