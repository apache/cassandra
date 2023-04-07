package org.apache.cassandra.audit.es.dto;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class EsResDto {

    private int took;
    private boolean timed_out;
    private _shards _shards;
    private Hits hits;
}
