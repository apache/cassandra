/*
 * Copyright DataStax, Inc.
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

package org.apache.cassandra.index.sai.disk;

import java.nio.ByteBuffer;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Wide-table tests (with clustering columns) for
 * {@link PrimaryKeyMap} using the row-aware on-disk format.
 */
public class RowAwareWidePrimaryKeyMapTest extends SAITester.Versioned.RowAware
{
    private final IndexContext intContext = SAITester.createIndexContext("int_index", Int32Type.instance);
    private final IndexContext textContext = SAITester.createIndexContext("text_index", UTF8Type.instance);

    private PrimaryKey.Factory pkFactory;
    private IPartitioner partitioner;
    private PrimaryKeyMap.Factory factory;
    private PrimaryKeyMap map;

    private long idPk1Ck1;
    private long idPk1Ck10;

    @Before
    public void setup() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, int_value int, text_value text, PRIMARY KEY (pk, ck)) WITH CLUSTERING ORDER BY (ck ASC)");
        execute("CREATE CUSTOM INDEX int_index ON %s(int_value) USING 'StorageAttachedIndex'");
        execute("CREATE CUSTOM INDEX text_index ON %s(text_value) USING 'StorageAttachedIndex'");

        // Insert multiple rows per partition to exercise clustering ordering
        // Partition pk=1
        execute("INSERT INTO %s (pk, ck, int_value, text_value) VALUES (?, ?, ?, ?)", 1, 1, 11, "a1");
        execute("INSERT INTO %s (pk, ck, int_value, text_value) VALUES (?, ?, ?, ?)", 1, 2, 12, "a2");
        execute("INSERT INTO %s (pk, ck, int_value, text_value) VALUES (?, ?, ?, ?)", 1, 3, 13, "a3");
        execute("INSERT INTO %s (pk, ck, int_value, text_value) VALUES (?, ?, ?, ?)", 1, 10, 110, "a10");

        // Partition pk=2
        execute("INSERT INTO %s (pk, ck, int_value, text_value) VALUES (?, ?, ?, ?)", 2, 1, 21, "b1");
        execute("INSERT INTO %s (pk, ck, int_value, text_value) VALUES (?, ?, ?, ?)", 2, 5, 25, "b5");

        // Partition pk=1000
        execute("INSERT INTO %s (pk, ck, int_value, text_value) VALUES (?, ?, ?, ?)", 1000, 1, 1001, "c1");
        execute("INSERT INTO %s (pk, ck, int_value, text_value) VALUES (?, ?, ?, ?)", 1000, 2, 1002, "c2");

        // Flush to generate SSTable and SAI components
        flush();

        // Obtain the just-flushed SSTable
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();

        // Build IndexDescriptor from the live SSTable using the matching index contexts
        IndexDescriptor indexDescriptor = IndexDescriptor.load(sstable, Set.of(intContext, textContext));
        this.pkFactory = indexDescriptor.perSSTableComponents().version().onDiskFormat().newPrimaryKeyFactory(cfs.metadata.get().comparator);

        IndexComponents.ForRead perSSTableComponents = indexDescriptor.perSSTableComponents();
        this.partitioner = sstable.metadata().partitioner;
        this.factory = perSSTableComponents.onDiskFormat().newPrimaryKeyMapFactory(perSSTableComponents, pkFactory, sstable);

        try (PrimaryKeyMap map = factory.newPerSSTablePrimaryKeyMap())
        {
            idPk1Ck1 = map.ceiling(buildPk(1, 1));
            idPk1Ck10 = map.ceiling(buildPk(1, 10));
        }

        map = factory.newPerSSTablePrimaryKeyMap();
    }

    @After
    public void tearDown() throws Exception
    {
        if (map != null)
            map.close();
        if (factory != null)
            factory.close();
    }

    @Test
    public void testExactRowIdOrInvertedCeiling()
    {
        assertThat(map.exactRowIdOrInvertedCeiling(beforeFirst(map))).as("before first expects the inverted first")
                                                                     .isEqualTo(~0);

        assertThat(map.exactRowIdOrInvertedCeiling(exactFirstRow(map))).as("exact first row")
                                                                       .isEqualTo(0);

        assertThat(map.exactRowIdOrInvertedCeiling(buildPk(1, 1))).as("exact pk=1, ck=1")
                                                                  .isEqualTo(idPk1Ck1);

        assertThat(map.exactRowIdOrInvertedCeiling(buildPk(1, 2))).as("exact pk=1, ck=2 expects next after pk=1, ck=1")
                                                                  .isEqualTo(idPk1Ck1 + 1);

        assertThat(map.exactRowIdOrInvertedCeiling(buildPk(1, 3))).as("exact pk=1, ck=3 expects next after pk=1, ck=2")
                                                                  .isEqualTo(idPk1Ck1 + 2);

        assertThat(map.exactRowIdOrInvertedCeiling(buildPk(1, 4))).as("between pk=1 ck=3 and ck=10 expects inverted ck=10")
                                                                  .isEqualTo(~idPk1Ck10);

        assertThat(map.exactRowIdOrInvertedCeiling(buildPk(1, 10))).as("exact pk=1, ck=10 expects next after pk=1, ck=3")
                                                                   .isEqualTo(idPk1Ck1 + 3);

        assertThat(map.exactRowIdOrInvertedCeiling(buildPk(1, Integer.MAX_VALUE))).as("after last ck in pk=1 expects inverted next partition first row or out of range if the last partition")
                                                                                  .isEqualTo(idPk1Ck10 < map.count()
                                                                                             ? ~(idPk1Ck10 + 1)
                                                                                             : Integer.MAX_VALUE);

        assertThat(map.exactRowIdOrInvertedCeiling(exactLastRow(map))).as("exact last row")
                                                                      .isEqualTo(map.count() - 1);

        if (Version.current(KEYSPACE).onOrAfter(Version.GA)) // See CNDB-18024
            assertThat(map.exactRowIdOrInvertedCeiling(buildPk(1000, 11))).as("after last row in last partition expects out of range")
                                                                          .isEqualTo(Long.MIN_VALUE);

        assertThat(map.exactRowIdOrInvertedCeiling(afterLastToken(map))).as("after last expects out of range")
                                                                        .isEqualTo(Long.MIN_VALUE);
    }

    @Test
    public void testCeiling()
    {
        assertThat(map.ceiling(beforeFirst(map))).as("before first expects the first")
                                                 .isEqualTo(0);

        assertThat(map.ceiling(exactFirstRow(map))).as("exact first row")
                                                   .isEqualTo(0);

        assertThat(map.ceiling(buildPk(1, 1))).as("exact pk=1, ck=1")
                                              .isEqualTo(idPk1Ck1);

        assertThat(map.ceiling(buildPk(1, 2))).as("pk=1, ck=2 expects next after pk=1, ck=1")
                                              .isEqualTo(idPk1Ck1 + 1);

        assertThat(map.ceiling(buildPk(1, 3))).as("exact pk=1, ck=3 expects next after pk=1, ck=2")
                                              .isEqualTo(idPk1Ck1 + 2);

        assertThat(map.ceiling(buildPk(1, 4))).as("between pk=1 ck=3 and ck=10 expects ck=10")
                                              .isEqualTo(idPk1Ck1 + 3);

        assertThat(map.ceiling(buildPk(1, 10))).as("exact pk=1, ck=10 expects next after pk=1, ck=3")
                                               .isEqualTo(idPk1Ck1 + 3);

        assertThat(map.ceiling(buildPk(1, Integer.MAX_VALUE))).as("after last ck in pk=1 expects inverted next partition first row or out of range if the last partition")
                                                              .isEqualTo(idPk1Ck10 < map.count()
                                                                         ? idPk1Ck10 + 1
                                                                         : -1);

        assertThat(map.ceiling(exactLastRow(map))).as("exact last row")
                                                  .isEqualTo(map.count() - 1);

        if (Version.current(KEYSPACE).onOrAfter(Version.GA)) // See CNDB-18024
            assertThat(map.ceiling(buildPk(1000, 11))).as("after last row in last partition expects out of range")
                                                      .isEqualTo(-1);

        assertThat(map.ceiling(afterLastToken(map))).as("after last expects out of range")
                                                    .isEqualTo(-1);
    }

    @Test
    public void testFloor()
    {
        assertThat(map.floor(beforeFirst(map))).as("before first expects out of range")
                                               .isEqualTo(-1);

        assertThat(map.floor(exactFirstRow(map))).as("exact first row")
                                                 .isEqualTo(0);

        assertThat(map.floor(buildPk(1, 1))).as("exact pk=1, ck=1")
                                            .isEqualTo(idPk1Ck1);

        assertThat(map.floor(buildPk(1, 2))).as("pk=1, ck=2 expects next after pk=1, ck=1")
                                            .isEqualTo(idPk1Ck1 + 1);

        assertThat(map.floor(buildPk(1, 3))).as("exact pk=1, ck=3 expects next after pk=1, ck=2")
                                            .isEqualTo(idPk1Ck1 + 2);

        assertThat(map.floor(buildPk(1, 10))).as("exact pk=1, ck=10 expects next after pk=1, ck=3")
                                             .isEqualTo(idPk1Ck1 + 3);

        assertThat(map.floor(buildPk(1, 4))).as("between pk=1 ck=3 and ck=10 expects ck=3")
                                            .isEqualTo(idPk1Ck1 + 2);

        assertThat(map.floor(buildPk(1, Integer.MAX_VALUE))).as("after last ck in pk=1 expects last ck in pk=1")
                                                            .isEqualTo(idPk1Ck10);

        assertThat(map.floor(exactLastRow(map))).as("exact last row")
                                                .isEqualTo(map.count() - 1);

        assertThat(map.floor(buildPk(1000, 11))).as("after last row in last partition expects the last")
                                                .isEqualTo(map.count() - 1);

        if (Version.current(KEYSPACE).onOrAfter(Version.GA)) // See CNDB-18024
            assertThat(map.floor(buildPk(1000, 11))).as("after last row in last partition expects the last row")
                                                    .isEqualTo(map.count() - 1);

        assertThat(map.floor(afterLastToken(map))).as("after last token expects the last")
                                                  .isEqualTo(map.count() - 1);
    }

    private PrimaryKey buildPk(int partitionKey, int clusteringKey)
    {
        ByteBuffer pkBuf = Int32Type.instance.decompose(partitionKey);
        DecoratedKey key = partitioner.decorateKey(pkBuf);
        Clustering<ByteBuffer> clustering = Clustering.make(Int32Type.instance.decompose(clusteringKey));
        return pkFactory.create(key, clustering);
    }

    private PrimaryKey beforeFirst(PrimaryKeyMap map)
    {
        PrimaryKey firstPk = map.primaryKeyFromRowId(0);
        long firstToken = firstPk.token().getLongValue();
        return pkFactory.createTokenOnly(partitioner.getTokenFactory().fromLongValue(firstToken - 1));
    }

    private PrimaryKey exactFirstRow(PrimaryKeyMap map)
    {
        return map.primaryKeyFromRowId(0);
    }

    private PrimaryKey exactLastRow(PrimaryKeyMap map)
    {
        return map.primaryKeyFromRowId(map.count() - 1);
    }

    private PrimaryKey afterLastToken(PrimaryKeyMap map)
    {
        PrimaryKey lastPk = map.primaryKeyFromRowId(map.count() - 1);
        long lastToken = lastPk.token().getLongValue();
        return pkFactory.createTokenOnly(partitioner.getTokenFactory().fromLongValue(lastToken + 1));
    }
}
