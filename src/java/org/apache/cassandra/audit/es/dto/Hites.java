package org.apache.cassandra.audit.es.dto;

import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;
import java.util.Map;

@Data
@Builder
public class Hites {
    private String _index;
    private String _id;
    private BigDecimal _score;
    private Map<String,Object> _source;
}
