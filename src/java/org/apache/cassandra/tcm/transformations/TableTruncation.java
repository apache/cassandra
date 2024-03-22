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

package org.apache.cassandra.tcm.transformations;

import java.io.IOException;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.ownership.Truncations.TruncationRecord;
import org.apache.cassandra.tcm.sequences.LockedRanges;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

public class TableTruncation implements Transformation
{
    public static final Serializer serializer = new Serializer();

    private final TableId tableId;
    private final long truncationTimestamp;

    public TableTruncation(TableId tableId, long truncationTimestamp)
    {
        this.tableId = tableId;
        this.truncationTimestamp = truncationTimestamp;
    }

    @Override
    public Kind kind()
    {
        return Kind.TRUNCATE_TABLE;
    }

    @Override
    public String toString()
    {
        return "TableTruncation{" +
               "tableId=" + tableId +
               ", truncationTimestamp=" + truncationTimestamp +
               '}';
    }

    @Override
    public Result execute(ClusterMetadata prev)
    {
        return Transformation.success(prev.transformer().truncateTable(tableId, new TruncationRecord(truncationTimestamp)),
                                      LockedRanges.AffectedRanges.EMPTY);
    }

    static class Serializer implements MetadataSerializer<Transformation>
    {

        @Override
        public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
        {
            TableTruncation truncation = (TableTruncation) t;
            truncation.tableId.serialize(out);
            out.writeLong(truncation.truncationTimestamp);
        }

        @Override
        public Transformation deserialize(DataInputPlus in, Version version) throws IOException
        {
            TableId id = TableId.deserialize(in);
            long truncationTimestamp = in.readLong();
            return new TableTruncation(id, truncationTimestamp);
        }

        @Override
        public long serializedSize(Transformation t, Version version)
        {
            TableTruncation truncation = (TableTruncation) t;
            return TypeSizes.sizeof(truncation.truncationTimestamp) + 16; // 16 is for UUID
        }
    }
}
