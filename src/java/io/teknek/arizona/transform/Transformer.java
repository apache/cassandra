package io.teknek.arizona.transform;

import java.util.Map;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ColumnFamily;

public interface Transformer {
  public ColumnFamily transform(ColumnFamily source, Map<String,String> properties, CFMetaData cfm);
}
