package io.teknek.arizona.transform;


import java.nio.charset.CharacterCodingException;
import java.util.Map;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ArrayBackedSortedColumns;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNames;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

public class Increment implements Transformer {
  @Override
  public ColumnFamily transform(ColumnFamily source, Map<String, String> properties, CFMetaData cfm) {
    ColumnFamily updates = ArrayBackedSortedColumns.factory.create(cfm);
    if (source == null){
      return updates;
    }
    for (Cell cell : source.getSortedColumns()) {
      CellName cn = CellNames.simpleDense(cell.name().get(0));
      int newValue = 0;
      try {
        newValue = Integer.parseInt(ByteBufferUtil.string(cell.value())) +1;
      } catch (NumberFormatException | CharacterCodingException e) {
        e.printStackTrace();
        return updates;
      }
      updates.addColumn(cn, ByteBufferUtil.bytes(newValue+""), FBUtilities.timestampMicros());
    }
    return updates;
  }

}