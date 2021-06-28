find /var/lib/cassandra/data/ -type f | grep -v -- -ib- | grep -v "/snapshots"
