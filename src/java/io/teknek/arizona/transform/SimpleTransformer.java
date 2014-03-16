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

public class SimpleTransformer implements Transformer {
  @Override
  public ColumnFamily transform(ColumnFamily source, Map<String, String> properties, CFMetaData cfm) {
    ColumnFamily updates = ArrayBackedSortedColumns.factory.create(cfm);
    if (source == null){
      return updates;
    }
    int i = 0;
    for (Cell cell : source.getSortedColumns()) {
      System.out.println(cell.name());
      try {
        System.out.println(ByteBufferUtil.string(cell.name().get(0)));
      } catch (CharacterCodingException e1) {
        // TODO Auto-generated catch block
        e1.printStackTrace();
      }
      try {
        System.out.println(ByteBufferUtil.string(cell.value()));
      } catch (CharacterCodingException e) {
      }
      CellName cn = CellNames.simpleDense(cell.name().get(0));
      updates.addColumn(cn, ByteBufferUtil.bytes(i+""), FBUtilities.timestampMicros());
    }
    return updates;
  }

}
