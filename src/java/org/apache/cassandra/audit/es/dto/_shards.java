package org.apache.cassandra.audit.es.dto;


import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class _shards {
    private int total;
    private int successful;
    private int skipped;
    private int failed;
}
