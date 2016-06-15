/*
 *
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
 *
 */
package org.apache.cassandra.index.internal.keys;

import java.nio.ByteBuffer;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.internal.CassandraIndex;
import org.apache.cassandra.index.internal.IndexEntry;
import org.apache.cassandra.schema.IndexMetadata;

public class KeysIndex extends CassandraIndex
{
    public KeysIndex(ColumnFamilyStore baseCfs, IndexMetadata indexDef)
    {
        super(baseCfs, indexDef);
    }

    public CFMetaData.Builder addIndexClusteringColumns(CFMetaData.Builder builder,
                                                        CFMetaData baseMetadata,
                                                        ColumnDefinition cfDef)
    {
        // no additional clustering columns required
        return builder;
    }

    protected CBuilder buildIndexClusteringPrefix(ByteBuffer partitionKey,
                                               ClusteringPrefix prefix,
                                               CellPath path)
    {
        CBuilder builder = CBuilder.create(getIndexComparator());
        builder.add(partitionKey);
        return builder;
    }

    protected ByteBuffer getIndexedValue(ByteBuffer partitionKey,
                                      Clustering clustering,
                                      CellPath path, ByteBuffer cellValue)
    {
        return cellValue;
    }

    public IndexEntry decodeEntry(DecoratedKey indexedValue, Row indexEntry)
    {
        throw new UnsupportedOperationException("KEYS indexes do not use a specialized index entry format");
    }

    public boolean isStale(Row row, ByteBuffer indexValue, int nowInSec)
    {
        if (row == null)
            return true;

        Cell cell = row.getCell(indexedColumn);

        return (cell == null
             || !cell.isLive(nowInSec)
             || indexedColumn.type.compare(indexValue, cell.value()) != 0);
    }
}
