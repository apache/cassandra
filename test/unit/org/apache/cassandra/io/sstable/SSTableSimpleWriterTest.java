/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.io.sstable;

import java.io.File;

import org.apache.cassandra.config.Schema;
import org.apache.cassandra.dht.IPartitioner;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.service.StorageService;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.apache.cassandra.utils.ByteBufferUtil.toInt;

public class SSTableSimpleWriterTest extends SchemaLoader
{
    @Test
    public void testSSTableSimpleUnsortedWriter() throws Exception
    {
        final int INC = 5;
        final int NBCOL = 10;

        String keyspaceName = "Keyspace1";
        String cfname = "StandardInteger1";

        Keyspace t = Keyspace.open(keyspaceName); // make sure we create the directory
        File dir = new Directories(Schema.instance.getCFMetaData(keyspaceName, cfname)).getDirectoryForNewSSTables();
        assert dir.exists();

        IPartitioner partitioner = StorageService.getPartitioner();
        SSTableSimpleUnsortedWriter writer = new SSTableSimpleUnsortedWriter(dir, partitioner, keyspaceName, cfname, IntegerType.instance, null, 16);

        int k = 0;

        // Adding a few rows first
        for (; k < 10; ++k)
        {
            writer.newRow(bytes("Key" + k));
            writer.addColumn(bytes(1), bytes("v"), 0);
            writer.addColumn(bytes(2), bytes("v"), 0);
            writer.addColumn(bytes(3), bytes("v"), 0);
        }


        // Testing multiple opening of the same row
        // We'll write column 0, 5, 10, .., on the first row, then 1, 6, 11, ... on the second one, etc.
        for (int i = 0; i < INC; ++i)
        {
            writer.newRow(bytes("Key" + k));
            for (int j = 0; j < NBCOL; ++j)
            {
                writer.addColumn(bytes(i + INC * j), bytes("v"), 1);
            }
        }
        k++;

        // Adding a few more rows
        for (; k < 20; ++k)
        {
            writer.newRow(bytes("Key" + k));
            writer.addColumn(bytes(1), bytes("v"), 0);
            writer.addColumn(bytes(2), bytes("v"), 0);
            writer.addColumn(bytes(3), bytes("v"), 0);
        }

        writer.close();

        // Now add that newly created files to the column family
        ColumnFamilyStore cfs = t.getColumnFamilyStore(cfname);
        cfs.loadNewSSTables();

        // Check we get expected results
        ColumnFamily cf = Util.getColumnFamily(t, Util.dk("Key10"), cfname);
        assert cf.getColumnCount() == INC * NBCOL : "expecting " + (INC * NBCOL) + " columns, got " + cf.getColumnCount();
        int i = 0;
        for (Cell c : cf)
        {
            assert toInt(c.name().toByteBuffer()) == i : "Cell name should be " + i + ", got " + toInt(c.name().toByteBuffer());
            assert c.value().equals(bytes("v"));
            assert c.timestamp() == 1;
            ++i;
        }

        cf = Util.getColumnFamily(t, Util.dk("Key19"), cfname);
        assert cf.getColumnCount() == 3 : "expecting 3 columns, got " + cf.getColumnCount();
    }
}
