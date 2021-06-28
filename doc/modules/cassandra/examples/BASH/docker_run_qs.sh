docker run --rm -it -v /<currentdir>/scripts:/scripts  \
-v /<currentdir/cqlshrc:/.cassandra/cqlshrc  \
--env CQLSH_HOST=host.docker.internal --env CQLSH_PORT=9042  nuvo/docker-cqlsh
