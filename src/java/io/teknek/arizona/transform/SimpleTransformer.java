package io.teknek.arizona.transform;


import java.nio.charset.CharacterCodingException;
import java.util.Map;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ArrayBackedSortedColumns;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.utils.ByteBufferUtil;

public class SimpleTransformer implements Transformer {
  @Override
  public ColumnFamily transform(ColumnFamily source, Map<String, String> properties, CFMetaData cfm) {
    ColumnFamily updates = ArrayBackedSortedColumns.factory.create(cfm);
    for (Cell cell : source.getSortedColumns()) {
      System.out.println(cell.name());
      try {
        System.out.println(ByteBufferUtil.string(cell.value()));
      } catch (CharacterCodingException e) {
      }
    }
    return updates;
  }

}
