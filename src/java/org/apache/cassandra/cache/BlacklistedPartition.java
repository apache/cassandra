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

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;

/**
 * Class to represent a blacklisted partition
 */
public class BlacklistedPartition implements IMeasurableMemory
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new BlacklistedPartition(null, new byte[0]));
    public final byte[] key;
    public final TableId tableId;

    public BlacklistedPartition(TableId tableId, DecoratedKey key)
    {
        this.tableId = tableId;
        this.key = ByteBufferUtil.getArray(key.getKey());
    }

    private BlacklistedPartition(TableId tableId, byte[] key)
    {
        this.tableId = tableId;
        this.key = key;
    }

    /**
     * Creates an instance of BlacklistedPartition for a given keyspace, table and partition key
     *
     * @param keyspace
     * @param table
     * @param key
     * @throws IllegalArgumentException
     */
    public BlacklistedPartition(String keyspace, String table, String key) throws IllegalArgumentException
    {
        // Determine tableId from keyspace and table parameters. If tableId cannot be determined due to invalid
        // parameters, throw an exception
        KeyspaceMetadata ksMetaData = Schema.instance.getKeyspaceMetadata(keyspace);
        if (ksMetaData == null)
        {
            throw new IllegalArgumentException("Unknown keyspace '" + keyspace + "'");
        }

        TableMetadata metadata = ksMetaData.getTableOrViewNullable(table);
        if (metadata == null)
        {
            throw new IllegalArgumentException("Unknown table '" + table + "' in keyspace '" + keyspace + "'");
        }

        ByteBuffer keyAsBytes = metadata.partitionKeyType.fromString(key);
        this.tableId = metadata.id;
        this.key = keyAsBytes.array();
    }

    /**
     * returns size in bytes of BlacklistedPartition instance
     *
     * @return
     */
    public long unsharedHeapSize()
    {
        return EMPTY_SIZE + ObjectSizes.sizeOf(tableId.toString()) + ObjectSizes.sizeOfArray(key);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BlacklistedPartition that = (BlacklistedPartition) o;

        return tableId.equals(that.tableId)
               && Arrays.equals(key, that.key);
    }

    @Override
    public int hashCode()
    {
        int result = tableId.hashCode();
        result = 31 * result + (key != null ? Arrays.hashCode(key) : 0);
        return result;
    }

    @Override
    public String toString()
    {
        TableMetadataRef tableRef = Schema.instance.getTableMetadataRef(tableId);
        return String.format("BlacklistedPartition(%s, key:%s)", tableRef, Arrays.toString(key));
    }
}
