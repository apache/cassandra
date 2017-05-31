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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;

public final class RowCacheKey extends CacheKey
{
    public final byte[] key;

    private static final long EMPTY_SIZE = ObjectSizes.measure(new RowCacheKey(null, null, new byte[0]));

    public RowCacheKey(TableId tableId, String indexName, byte[] key)
    {
        super(tableId, indexName);
        this.key = key;
    }

    public RowCacheKey(TableMetadata metadata, DecoratedKey key)
    {
        super(metadata);
        this.key = ByteBufferUtil.getArray(key.getKey());
        assert this.key != null;
    }

    @VisibleForTesting
    public RowCacheKey(TableId tableId, String indexName, ByteBuffer key)
    {
        super(tableId, indexName);
        this.key = ByteBufferUtil.getArray(key);
        assert this.key != null;
    }

    public long unsharedHeapSize()
    {
        return EMPTY_SIZE + ObjectSizes.sizeOfArray(key);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RowCacheKey that = (RowCacheKey) o;

        return tableId.equals(that.tableId)
               && Objects.equals(indexName, that.indexName)
               && Arrays.equals(key, that.key);
    }

    @Override
    public int hashCode()
    {
        int result = tableId.hashCode();
        result = 31 * result + Objects.hashCode(indexName);
        result = 31 * result + (key != null ? Arrays.hashCode(key) : 0);
        return result;
    }

    @Override
    public String toString()
    {
        TableMetadataRef tableRef = Schema.instance.getTableMetadataRef(tableId);
        return String.format("RowCacheKey(%s, %s, key:%s)", tableRef, indexName, Arrays.toString(key));
    }
}
