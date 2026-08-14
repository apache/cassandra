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

package org.apache.cassandra.index.sai.disk.v2;

import io.github.jbellis.jvector.util.RamUsageEstimator;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.sstable.SSTableId;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

public class PrimaryKeyWithSource implements PrimaryKey
{
    private final SSTableId<?> sourceSstableId;
    private final long sourceRowId;
    private PrimaryKey delegatePrimaryKey;
    private PrimaryKeyMap primaryKeyMap;
    private final PrimaryKey sourceSstableMinKey;
    private final PrimaryKey sourceSstableMaxKey;

    public PrimaryKeyWithSource(PrimaryKeyMap primaryKeyMap, long sstableRowId, PrimaryKey sourceSstableMinKey, PrimaryKey sourceSstableMaxKey)
    {
        this.primaryKeyMap = primaryKeyMap;
        this.sourceSstableId = primaryKeyMap.getSSTableId();
        this.sourceRowId = sstableRowId;
        this.sourceSstableMinKey = sourceSstableMinKey;
        this.sourceSstableMaxKey = sourceSstableMaxKey;
    }

    private PrimaryKey primaryKey()
    {
        if (delegatePrimaryKey == null)
        {
            delegatePrimaryKey = primaryKeyMap.primaryKeyFromRowId(sourceRowId);
            primaryKeyMap = null; // Removes the no longer needed reference to the primary key map.
        }

        return delegatePrimaryKey;
    }

    public long getSourceRowId()
    {
        return sourceRowId;
    }

    public SSTableId<?> getSourceSstableId()
    {
        return sourceSstableId;
    }

    @Override
    public PrimaryKey forStaticRow()
    {
        // We cannot use row awareness if we need a static row.
        return primaryKey().forStaticRow();
    }

    @Override
    public Token token()
    {
        return primaryKey().token();
    }

    @Override
    public DecoratedKey partitionKey()
    {
        return primaryKey().partitionKey();
    }

    @Override
    public Clustering<?> clustering()
    {
        return primaryKey().clustering();
    }

    @Override
    public PrimaryKey loadDeferred()
    {
        primaryKey().loadDeferred();
        return this;
    }

    @Override
    public ByteSource asComparableBytes(ByteComparable.Version version)
    {
        return primaryKey().asComparableBytes(version);
    }

    @Override
    public ByteSource asComparableBytesMinPrefix(ByteComparable.Version version)
    {
        return primaryKey().asComparableBytesMinPrefix(version);
    }

    @Override
    public ByteSource asComparableBytesMaxPrefix(ByteComparable.Version version)
    {
        return primaryKey().asComparableBytesMaxPrefix(version);
    }

    @Override
    public int compareTo(PrimaryKey o)
    {
        if (o instanceof PrimaryKeyWithSource)
        {
            var other = (PrimaryKeyWithSource) o;
            if (sourceSstableId.equals(other.sourceSstableId))
                return Long.compare(sourceRowId, other.sourceRowId);
            // Compare to the other source sstable's min and max keys to determine if the keys are comparable.
            // Note that these are already loaded into memory as part of the segment's metadata, so the comparison
            // is cheaper than loading the actual keys.
            if (sourceSstableMinKey.compareTo(other.sourceSstableMaxKey) > 0)
                return 1;
            if (sourceSstableMaxKey.compareTo(other.sourceSstableMinKey) < 0)
                return -1;
        }
        else
        {
            if (sourceSstableMinKey.compareTo(o) > 0)
                return 1;
            if (sourceSstableMaxKey.compareTo(o) < 0)
                return -1;
        }

        return primaryKey().compareTo(o);
    }

    @Override
    public boolean equals(Object o)
    {
        if (o instanceof PrimaryKeyWithSource)
        {
            var other = (PrimaryKeyWithSource) o;
            // If they are from the same source sstable, we can compare the row ids directly.
            if (sourceSstableId.equals(other.sourceSstableId))
                return sourceRowId == other.sourceRowId;

            // If the source sstable primary key ranges do not intersect, the keys cannot be equal.
            if (sourceSstableMinKey.compareTo(other.sourceSstableMaxKey) > 0
                || sourceSstableMaxKey.compareTo(other.sourceSstableMinKey) < 0)
                return false;
        }

        return primaryKey().equals(o);
    }

    @Override
    public int hashCode()
    {
        return primaryKey().hashCode();
    }

    @Override
    public String toString()
    {
        return String.format("%s (source sstable: %s, %s)", delegatePrimaryKey, sourceSstableId, sourceRowId);
    }

    @Override
    public long ramBytesUsed()
    {
        // Object header + 3 references (primaryKey, sourceSstableId) + long value
        return RamUsageEstimator.NUM_BYTES_OBJECT_HEADER +
               2L * RamUsageEstimator.NUM_BYTES_OBJECT_REF +
               Long.BYTES +
               primaryKey().ramBytesUsed();
    }
}
