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
package org.apache.cassandra.cache;

import java.util.Objects;

import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;

public abstract class CacheKey implements IMeasurableMemory
{
    public final TableId tableId;
    public final String indexName;

    protected CacheKey(TableId tableId, String indexName)
    {
        this.tableId = tableId;
        this.indexName = indexName;
    }

    public CacheKey(TableMetadata metadata)
    {
        this(metadata.id, metadata.indexName().orElse(null));
    }

    public boolean sameTable(TableMetadata tableMetadata)
    {
        return tableId.equals(tableMetadata.id)
               && Objects.equals(indexName, tableMetadata.indexName().orElse(null));
    }
}
