package org.apache.cassandra.hadoop;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.hadoop.conf.Configuration;

public class ColumnFamilyInputFormatTest
{
    @Test
    public void testSlicePredicate()
    {
        long columnValue = 1271253600000l;
        byte[] columnBytes = FBUtilities.toByteArray(columnValue);

        List<byte[]> columnNames = new ArrayList<byte[]>();
        columnNames.add(columnBytes);
        SlicePredicate originalPredicate = new SlicePredicate().setColumn_names(columnNames);

        Configuration conf = new Configuration();
        ConfigHelper.setSlicePredicate(conf, originalPredicate);

        SlicePredicate rtPredicate = ConfigHelper.getSlicePredicate(conf);
        assert rtPredicate.column_names.size() == 1;
        assert Arrays.equals(originalPredicate.column_names.get(0), rtPredicate.column_names.get(0));
    }
}
