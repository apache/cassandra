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

package org.apache.cassandra.index.internal;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import accord.utils.Gen;
import accord.utils.RandomSource;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static accord.utils.Property.qt;
import static org.junit.Assert.assertEquals;

/**
 * A tracked index read builds entries with {@link CassandraIndex#createIndexEntry} and the searcher uses them
 * interchangeably with ones decoded from the index row, so the two must agree for every table and index shape.
 */
public class CassandraIndexEntryRoundTripTest extends CQLTester
{
    private static final Gen<ByteBuffer> INTS = rs -> Int32Type.instance.decompose(rs.nextInt());

    /**
     * Each of the 7 base tables yields (clustering columns + 2) entries from its regular row: one for the index on a
     * partition key column, one per index on a clustering column, one for the index on the regular column. Each of
     * the 3 tables with a static column yields 2 more from its static row: the index on a partition key column and
     * the index on the static column. Every other pair is one the write path skips.
     */
    private static final int ENTRIES_PER_EXAMPLE = 32;

    @BeforeClass
    public static void initialize()
    {
        StorageService.instance.unsafeSetInitialized();
    }

    @Test
    public void entriesDecodeToTheBaseRowTheyWereBuiltFrom()
    {
        List<CassandraIndex> indexes = new ArrayList<>();
        for (int clusterings = 0; clusterings <= 3; clusterings++)
        {
            for (boolean hasStatic : new boolean[]{ false, true })
            {
                if (hasStatic && clusterings == 0)
                    continue;

                StringBuilder columns = new StringBuilder("pk0 int, pk1 int, v int");
                StringBuilder key = new StringBuilder("(pk0, pk1)");
                List<String> targets = new ArrayList<>(Arrays.asList("pk1", "v"));
                for (int i = 0; i < clusterings; i++)
                {
                    columns.append(", ck").append(i).append(" int");
                    key.append(", ck").append(i);
                    targets.add("ck" + i);
                }
                if (hasStatic)
                {
                    columns.append(", s int static");
                    targets.add("s");
                }

                createTable(String.format("CREATE TABLE %%s (%s, PRIMARY KEY (%s))", columns, key));
                for (String target : targets)
                {
                    String name = createIndex(String.format("CREATE INDEX ON %%s(%s) USING '%s'", target, CassandraIndex.NAME));
                    indexes.add((CassandraIndex) getCurrentColumnFamilyStore().indexManager.getIndexByName(name));
                }
            }
        }

        qt().check(rs -> {
            int entries = 0;
            for (CassandraIndex index : indexes)
                for (boolean staticRow : new boolean[]{ false, true })
                    entries += roundTrip(index, staticRow, rs);
            assertEquals("entries the write path built", ENTRIES_PER_EXAMPLE, entries);
        });
    }

    private static int roundTrip(CassandraIndex index, boolean staticRow, RandomSource rs)
    {
        TableMetadata base = index.baseCfs.metadata();
        long now = FBUtilities.nowInSeconds();
        long timestamp = rs.nextLong(0, Long.MAX_VALUE);
        DecoratedKey key = index.baseCfs.decorateKey(CompositeType.build(ByteBufferAccessor.instance,
                                                                        INTS.next(rs), INTS.next(rs)));
        ByteBuffer[] values = new ByteBuffer[base.comparator.size()];
        for (int i = 0; i < values.length; i++)
            values[i] = INTS.next(rs);

        Row.Builder row = BTreeRow.unsortedBuilder();
        row.newRow(staticRow ? Clustering.STATIC_CLUSTERING : Clustering.make(values));
        if (!staticRow)
            row.addPrimaryKeyLivenessInfo(LivenessInfo.create(timestamp, now));
        ColumnMetadata column = base.getColumn(ByteBufferUtil.bytes(staticRow ? "s" : "v"));
        if (column != null)
            row.addCell(BufferCell.live(column, timestamp, INTS.next(rs)));

        int[] built = new int[1];
        new CassandraIndex.AbstractIndexer()
        {
            CassandraIndex index()
            {
                return index;
            }

            long nowInSec()
            {
                return now;
            }

            DecoratedKey key()
            {
                return key;
            }

            void insert(DecoratedKey rowKey, Clustering<?> clustering, Cell<?> cell, LivenessInfo info)
            {
                built[0]++;
                IndexEntry written = index.createIndexEntry(rowKey, clustering, cell, info);
                IndexEntry decoded = index.decodeEntry(written.indexValue,
                                                       BTreeRow.noCellLiveRow(written.indexClustering, info));
                String what = String.format("%s on %s column %s, base table with %d clustering column(s)%s," +
                                            " %s row, base key %s, index clustering %s:" +
                                            " wrote base clustering %s, decoded %s",
                                            index.getClass().getSimpleName(),
                                            index.getIndexedColumn().kind,
                                            index.getIndexedColumn().name,
                                            base.comparator.size(),
                                            base.hasStaticColumns() ? " and a static column" : "",
                                            staticRow ? "static" : "regular",
                                            base.partitionKeyType.getString(rowKey.getKey()),
                                            render(written.indexClustering, index.getIndexCfs().metadata()),
                                            render(written.indexedEntryClustering, base),
                                            render(decoded.indexedEntryClustering, base));
                assertEquals(what, written.indexedKey, decoded.indexedKey);
                assertEquals(what, written.indexedEntryClustering, decoded.indexedEntryClustering);
                assertEquals(what, 0, IndexEntry.compare(index.getIndexCfs().metadata(), base, written, decoded));
            }

            void delete(DecoratedKey rowKey, Clustering<?> clustering, Cell<?> cell, long nowInSec)
            {
            }

            void delete(DecoratedKey rowKey, Clustering<?> clustering, DeletionTime deletion)
            {
            }
        }.insertRow(row.build());

        return built[0];
    }

    private static String render(Clustering<?> clustering, TableMetadata metadata)
    {
        return clustering.kind() + "(" + clustering.toString(metadata) + ')';
    }
}
