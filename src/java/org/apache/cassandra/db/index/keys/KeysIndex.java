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
package org.apache.cassandra.db.index.keys;

import java.nio.ByteBuffer;
import java.util.Set;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.index.SecondaryIndex;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.index.AbstractSimplePerColumnSecondaryIndex;
import org.apache.cassandra.db.index.SecondaryIndexSearcher;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.concurrent.OpOrder;

/**
 * Implements a secondary index for a column family using a second column family.
 * The design uses inverted index http://en.wikipedia.org/wiki/Inverted_index.
 * The row key is the indexed value. For example, if we're indexing a column named
 * city, the index value of city is the row key.
 * The column names are the keys of the records. To see a detailed example, please
 * refer to wikipedia.
 */
public class KeysIndex extends AbstractSimplePerColumnSecondaryIndex
{
    public static void addIndexClusteringColumns(CFMetaData.Builder indexMetadata, CFMetaData baseMetadata, ColumnDefinition cfDef)
    {
        indexMetadata.addClusteringColumn("partition_key", SecondaryIndex.keyComparator);
    }

    @Override
    public void indexRow(DecoratedKey key, Row row, OpOrder.Group opGroup, int nowInSec)
    {
        super.indexRow(key, row, opGroup, nowInSec);

        // This is used when building indexes, in particular when the index is first created. On thrift, this
        // potentially means the column definition just got created, and so we need to check if's not a "dynamic"
        // row that actually correspond to the index definition.
        assert baseCfs.metadata.isCompactTable();
        if (!row.isStatic())
        {
            Clustering clustering = row.clustering();
            if (clustering.get(0).equals(columnDef.name.bytes))
            {
                Cell cell = row.getCell(baseCfs.metadata.compactValueColumn());
                if (cell != null && cell.isLive(nowInSec))
                    insert(key.getKey(), clustering, cell, opGroup);
            }
        }
    }

    protected ByteBuffer getIndexedValue(ByteBuffer rowKey, Clustering clustering, ByteBuffer cellValue, CellPath path)
    {
        return cellValue;
    }

    protected CBuilder buildIndexClusteringPrefix(ByteBuffer rowKey, ClusteringPrefix prefix, CellPath path)
    {
        CBuilder builder = CBuilder.create(getIndexComparator());
        builder.add(rowKey);
        return builder;
    }

    public SecondaryIndexSearcher createSecondaryIndexSearcher(Set<ColumnDefinition> columns)
    {
        return new KeysSearcher(baseCfs.indexManager, columns);
    }

    public void validateOptions() throws ConfigurationException
    {
        // no options used
    }
}
