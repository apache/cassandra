/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.columniterator;

import java.nio.ByteBuffer;
import java.util.Random;

import com.google.common.collect.Iterables;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.KeyspaceParams;

public class SSTableReverseIteratorTest
{
    private static final String KEYSPACE = "ks";
    private Random random;

    @BeforeClass
    public static void setupClass()
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1));
    }

    @Before
    public void setUp()
    {
        random = new Random(0);
    }

    private ByteBuffer bytes(int size)
    {
        byte[] b = new byte[size];
        random.nextBytes(b);
        return ByteBuffer.wrap(b);
    }

    /**
     * SSTRI shouldn't bail out if it encounters empty blocks (due to dropped columns)
     */
    @Test
    public void emptyBlockTolerance()
    {
        String table = "empty_block_tolerance";
        QueryProcessor.executeInternal(String.format("CREATE TABLE %s.%s (k INT, c int, v1 blob, v2 blob, primary key (k, c))", KEYSPACE, table));
        ColumnFamilyStore tbl = Keyspace.open(KEYSPACE).getColumnFamilyStore(table);
        assert tbl != null;

        int key = 100;

        QueryProcessor.executeInternal(String.format("UPDATE %s.%s SET v1=?, v2=? WHERE k=? AND c=?", KEYSPACE, table), bytes(8), bytes(8), key, 0);
        QueryProcessor.executeInternal(String.format("UPDATE %s.%s SET v1=? WHERE k=? AND c=?", KEYSPACE, table), bytes(0x20000), key, 1);
        QueryProcessor.executeInternal(String.format("UPDATE %s.%s SET v1=? WHERE k=? AND c=?", KEYSPACE, table), bytes(0x20000), key, 2);
        QueryProcessor.executeInternal(String.format("UPDATE %s.%s SET v1=? WHERE k=? AND c=?", KEYSPACE, table), bytes(0x20000), key, 3);

        tbl.forceBlockingFlush();
        SSTableReader sstable = Iterables.getOnlyElement(tbl.getLiveSSTables());
        DecoratedKey dk = tbl.getPartitioner().decorateKey(Int32Type.instance.decompose(key));
        RowIndexEntry indexEntry = sstable.getPosition(dk, SSTableReader.Operator.EQ);
        Assert.assertTrue(indexEntry.isIndexed());
        Assert.assertTrue(indexEntry.columnsIndexCount() > 2);

        // drop v1 so the first 2 index blocks only contain empty unfiltereds
        QueryProcessor.executeInternal(String.format("ALTER TABLE %s.%s DROP v1", KEYSPACE, table));

        UntypedResultSet result = QueryProcessor.executeInternal(String.format("SELECT v2 FROM %s.%s WHERE k=? ORDER BY c DESC", KEYSPACE, table), key);
        Assert.assertEquals(1, result.size());

    }
}
