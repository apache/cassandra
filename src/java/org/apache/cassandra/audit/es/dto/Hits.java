package org.apache.cassandra.audit.es.dto;

import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
@Builder
public class Hits {
    private Total total;
    private double max_score;
    private List<Hites> hits;
}
