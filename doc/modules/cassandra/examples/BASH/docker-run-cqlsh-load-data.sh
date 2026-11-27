docker run --rm --network cassandra \
-v "$(pwd)/data.cql:/scripts/data.cql" \
-e CQLSH_HOST=cassandra -e CQLSH_PORT=9042 \
-e CQLVERSION=3.4.7 nuvo/docker-cqlsh