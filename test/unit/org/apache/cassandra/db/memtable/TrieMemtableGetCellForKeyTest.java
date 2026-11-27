/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.memtable;

import java.util.BitSet;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.dht.BootStrapper;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/// Checks that [TrieMemtable#getCellForKey], which reads only the shard owning the partition, finds the cells of
/// partitions living in every shard, including static cells, and nothing for deleted or missing data.
public class TrieMemtableGetCellForKeyTest extends CQLTester
{
    private static final int PARTITIONS = 2000;
    private static final int ROWS = 3;

    @BeforeClass
    public static void setUpTokens()
    {
        // the memtable is split in shards only when the node owns some ranges
        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        metadata.updateNormalTokens(BootStrapper.getRandomTokens(metadata, 10), FBUtilities.getBroadcastAddressAndPort());
    }

    @Test
    public void testGetCellForKeyAcrossShards()
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, s bigint static, v bigint, PRIMARY KEY (pk, ck))" +
                    " WITH memtable = 'trie'");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (long pk = 0; pk < PARTITIONS; pk++)
        {
            if (pk % 2 == 0)
                execute("INSERT INTO %s (pk, s) VALUES (?, ?)", pk, -pk);
            for (long ck = 0; ck < ROWS; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, pk * 10 + ck);
        }
        for (long pk = 0; pk < PARTITIONS; pk++)
        {
            if (pk % 5 == 1)
                execute("DELETE FROM %s WHERE pk = ? AND ck = ?", pk, 1L);
            else if (pk % 5 == 2)
                execute("DELETE FROM %s WHERE pk = ?", pk);
        }

        TrieMemtable memtable = (TrieMemtable) cfs.getCurrentMemtable();
        int shardCount = memtable.boundaries.shardCount();
        assertTrue("The test needs a memtable with more than one shard", shardCount > 1);

        ColumnMetadata v = cfs.metadata().getColumn(ByteBufferUtil.bytes("v"));
        ColumnMetadata s = cfs.metadata().getColumn(ByteBufferUtil.bytes("s"));
        BitSet shardsHit = new BitSet(shardCount);

        for (long pk = 0; pk < PARTITIONS; pk++)
        {
            DecoratedKey dk = cfs.getPartitioner().decorateKey(LongType.instance.decompose(pk));
            shardsHit.set(memtable.boundaries.getShardForKey(dk));
            boolean partitionDeleted = pk % 5 == 2;

            for (long ck = -1; ck <= ROWS; ck++)
            {
                Cell<?> cell = memtable.getCellForKey(dk, cfs.getComparator().make(ck), v);
                boolean present = ck >= 0 && ck < ROWS && !partitionDeleted && !(pk % 5 == 1 && ck == 1);
                if (present)
                {
                    assertNotNull("pk " + pk + " ck " + ck, cell);
                    assertEquals(pk * 10 + ck, LongType.instance.compose(cell.buffer()).longValue());
                }
                else
                    assertNull("pk " + pk + " ck " + ck, cell);
            }

            Cell<?> staticCell = memtable.getCellForKey(dk, Clustering.STATIC_CLUSTERING, s);
            if (pk % 2 == 0 && !partitionDeleted)
            {
                assertNotNull("static of pk " + pk, staticCell);
                assertEquals(-pk, LongType.instance.compose(staticCell.buffer()).longValue());
            }
            else
                assertNull("static of pk " + pk, staticCell);
        }
        assertEquals("Partitions must be spread over all shards", shardCount, shardsHit.cardinality());

        // a partition that was never written
        DecoratedKey missing = cfs.getPartitioner().decorateKey(LongType.instance.decompose((long) PARTITIONS));
        assertNull(memtable.getCellForKey(missing, cfs.getComparator().make(0L), v));
        assertNull(memtable.getCellForKey(missing, Clustering.STATIC_CLUSTERING, s));
    }
}
