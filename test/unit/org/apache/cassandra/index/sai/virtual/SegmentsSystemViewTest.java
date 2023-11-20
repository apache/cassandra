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
package org.apache.cassandra.index.sai.virtual;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.virtual.VirtualKeyspace;
import org.apache.cassandra.db.virtual.VirtualKeyspaceRegistry;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.SSTableIndex;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.segment.SegmentBuilder;
import org.apache.cassandra.index.sai.disk.v1.segment.SegmentMetadata;
import org.apache.cassandra.index.sai.utils.IndexIdentifier;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_ENCRYPTION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests the virtual table exposing SSTable index segment metadata.
 */
public class SegmentsSystemViewTest extends SAITester
{
    private static final String SELECT = String.format("SELECT %s, %s, %s, %s " +
                                                       "FROM %s.%s WHERE %s = '%s' AND %s = ?",
                                                       SegmentsSystemView.SEGMENT_ROW_ID_OFFSET,
                                                       SegmentsSystemView.CELL_COUNT,
                                                       SegmentsSystemView.MIN_SSTABLE_ROW_ID,
                                                       SegmentsSystemView.MAX_SSTABLE_ROW_ID,
                                                       SchemaConstants.VIRTUAL_VIEWS,
                                                       SegmentsSystemView.NAME,
                                                       SegmentsSystemView.KEYSPACE_NAME,
                                                       KEYSPACE,
                                                       SegmentsSystemView.INDEX_NAME);


    private static final String SELECT_INDEX_METADATA = String.format("SELECT %s, %s, %s " +
                                                                      "FROM %s.%s WHERE %s = '%s'",
                                                                      SegmentsSystemView.COMPONENT_METADATA,
                                                                      SegmentsSystemView.MIN_TERM,
                                                                      SegmentsSystemView.MAX_TERM,
                                                                      SchemaConstants.VIRTUAL_VIEWS,
                                                                      SegmentsSystemView.NAME,
                                                                      SegmentsSystemView.KEYSPACE_NAME,
                                                                      KEYSPACE);

    @BeforeClass
    public static void setup()
    {
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(SchemaConstants.VIRTUAL_VIEWS, ImmutableList.of(new SegmentsSystemView(SchemaConstants.VIRTUAL_VIEWS))));

        requireNetwork();
    }

    @Test
    public void testSegmentsMetadata() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v1 text, PRIMARY KEY (k, c))");
        String literalIndex = createIndex("CREATE CUSTOM INDEX ON %s(v1) USING 'StorageAttachedIndex'");

        int num = 100;

        String insert = "INSERT INTO %s(k, c, v1) VALUES (?, ?, ?)";

        // the virtual table should be empty before adding contents
        assertEmpty(execute(SELECT, literalIndex));

        // insert rows and verify that the virtual table is empty before flushing
        for (int i = 0; i < num / 2; i++)
            execute(insert, i, 10, "1000");
        assertEmpty(execute(SELECT, literalIndex));

        // flush the memtable and verify the new record in the virtual table
        flush();
        Object[] row1 = row(0L, (long)(num / 2), 0L, (long)(num / 2 - 1));
        assertRows(execute(SELECT, literalIndex), row1);

        // flush a second memtable and verify both the old and the new record in the virtual table
        for (int i = num / 2; i < num; i++)
            execute(insert, i, 20, "2000");
        flush();
        Object[] row2 = row(0L, (long)(num / 2), 0L, (long)(num / 2 - 1));
        assertRows(execute(SELECT, literalIndex), row1, row2);

        // force compaction, there is only 1 sstable
        compact();
        waitForCompactions();
        Object[] row3 = row(0L, (long)num, 0L, (long)(num - 1));
        assertRows(execute(SELECT, literalIndex), row3);

        for (int lastValidSegmentRowId : Arrays.asList(0, 1, 2, 3, 5, 9, 25, 49, 59, 99, 101))
        {
            SegmentBuilder.updateLastValidSegmentRowId(lastValidSegmentRowId);

            // compaction to rewrite segments
            StorageService.instance.upgradeSSTables(KEYSPACE, false, currentTable());

            List<Object[]> segmentRows = new ArrayList<>();

            for (int row = 0; row < num / (lastValidSegmentRowId + 1); row++)
                segmentRows.add(row((long)(row * (lastValidSegmentRowId + 1)),
                                    (long)(lastValidSegmentRowId + 1),
                                    (long)(row * (lastValidSegmentRowId + 1)),
                                    (long)(row * (lastValidSegmentRowId + 1) + lastValidSegmentRowId)));
            long prevMaxSSTableRowId = segmentRows.isEmpty() ? -1L : (long)segmentRows.get(segmentRows.size() - 1)[3];
            if (prevMaxSSTableRowId < 99L)
            {
                segmentRows.add(row(prevMaxSSTableRowId + 1, 99 - prevMaxSSTableRowId, prevMaxSSTableRowId + 1, 99L));
            }

            UntypedResultSet resultSet = execute(SELECT, literalIndex);
            assertRows(execute(SELECT, literalIndex), segmentRows.toArray(new Object[][]{}));
            // verify index metadata length
            Map<String, Long> indexLengths = new HashMap<>();
            for (UntypedResultSet.Row row : execute(SELECT_INDEX_METADATA))
            {
                int minTerm = Integer.parseInt(row.getString(SegmentsSystemView.MIN_TERM));
                int maxTerm = Integer.parseInt(row.getString(SegmentsSystemView.MAX_TERM));

                assertTrue(minTerm >= 100);
                assertTrue(maxTerm <= 2000);

                Map<String, Map<String, String>> indexMetadatas = row.getMap(SegmentsSystemView.COMPONENT_METADATA,
                                                                             UTF8Type.instance,
                                                                             MapType.getInstance(UTF8Type.instance, UTF8Type.instance, true));

                for (Map.Entry<String, Map<String, String>> entry : indexMetadatas.entrySet())
                {
                    final String indexType = entry.getKey();
                    final String str = entry.getValue().getOrDefault(SegmentMetadata.ComponentMetadata.LENGTH, "0");

                    final long length = Long.parseLong(str);

                    final long value = indexLengths.getOrDefault(indexType, 0L);
                    indexLengths.put(indexType, value + length);
                }
            }
            if (!TEST_ENCRYPTION.getBoolean())
                assertEquals(indexFileLengths(), indexLengths);
        }

        // drop the numeric index and verify that there are not entries for it in the table
        assertNotEquals(0, execute(SELECT, literalIndex).size());

        // drop the string index and verify that there are not entries for it in the table
        dropIndex("DROP INDEX %s." +  literalIndex);
        assertEmpty(execute(SELECT, literalIndex));
    }

    private HashMap<String, Long> indexFileLengths()
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        HashMap<String, Long> lengths = new HashMap<>();
        for (Index idx : cfs.indexManager.listIndexes())
        {
            StorageAttachedIndex index = (StorageAttachedIndex) idx;

            for (SSTableIndex sstableIndex : index.view().getIndexes())
            {
                SSTableReader sstable = sstableIndex.getSSTable();

                IndexDescriptor indexDescriptor = IndexDescriptor.create(sstable);
                indexDescriptor.hasComponent(IndexComponent.COLUMN_COMPLETION_MARKER, index.identifier());

                if (sstableIndex.getIndexTermType().isLiteral())
                {
                    addComponentSizeToMap(lengths, IndexComponent.TERMS_DATA, index.identifier(), indexDescriptor);
                    addComponentSizeToMap(lengths, IndexComponent.POSTING_LISTS, index.identifier(), indexDescriptor);
                }
            }
        }

        return lengths;
    }

    private static void addComponentSizeToMap(HashMap<String, Long> map, IndexComponent key, IndexIdentifier indexIdentifier, IndexDescriptor indexDescriptor)
    {
        map.compute(key.name(), (typeName, acc) -> {
            final long size = indexDescriptor.sizeOnDiskOfPerIndexComponent(key, indexIdentifier);
            return acc == null ? size : size + acc;
        });
    }
}
