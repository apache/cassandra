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
package org.apache.cassandra.replication.logged;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

import org.agrona.collections.Long2ObjectHashMap;
import org.apache.cassandra.db.Digest;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.replication.CoordinatorLogId;
import org.apache.cassandra.replication.MutationSummary;
import org.apache.cassandra.replication.Offsets;
import org.apache.cassandra.schema.TableId;

public class LoggedMutationSummary implements MutationSummary
{
    public static class CoordinatorSummary
    {
        private static final Comparator<CoordinatorSummary> idComparator = Comparator.comparing(o -> o.logId);

        public final CoordinatorLogId logId;
        public final Offsets reconciled;
        public final Offsets unreconciled;

        public CoordinatorSummary(CoordinatorLogId logId, Offsets reconciled, Offsets unreconciled)
        {
            this.logId = logId;
            this.reconciled = reconciled;
            this.unreconciled = unreconciled;
        }

        void digest(Digest digest)
        {
            digest.updateWithLong(logId.asLong());
            reconciled.digest(digest);
            unreconciled.digest(digest);
        }

        public static class Builder
        {
            public final CoordinatorLogId logId;
            public final Offsets reconciled = new Offsets();
            public final Offsets unreconciled = new Offsets();

            public Builder(CoordinatorLogId logId)
            {
                this.logId = logId;
            }

            public CoordinatorSummary build()
            {
                return new CoordinatorSummary(logId, reconciled, unreconciled);
            }
        }

        public static final IVersionedSerializer<CoordinatorSummary> serializer = new IVersionedSerializer<>()
        {
            @Override
            public void serialize(CoordinatorSummary t, DataOutputPlus out, int version) throws IOException
            {
                CoordinatorLogId.serializer.serialize(t.logId, out, version);
                Offsets.serializer.serialize(t.reconciled, out, version);
                Offsets.serializer.serialize(t.unreconciled, out, version);
            }

            @Override
            public CoordinatorSummary deserialize(DataInputPlus in, int version) throws IOException
            {
                return new CoordinatorSummary(CoordinatorLogId.serializer.deserialize(in, version),
                                              Offsets.serializer.deserialize(in, version),
                                              Offsets.serializer.deserialize(in, version));
            }

            @Override
            public long serializedSize(CoordinatorSummary t, int version)
            {
                return CoordinatorLogId.serializer.serializedSize(t.logId, version)
                       + Offsets.serializer.serializedSize(t.reconciled, version)
                       + Offsets.serializer.serializedSize(t.unreconciled, version);
            }
        };
    }

    public static class Builder
    {
        public final TableId tableId;
        private final Long2ObjectHashMap<CoordinatorSummary.Builder> builders = new Long2ObjectHashMap<>();

        public Builder(TableId tableId)
        {
            this.tableId = tableId;
        }

        public CoordinatorSummary.Builder builderForLog(CoordinatorLogId logId)
        {
            CoordinatorSummary.Builder builder = builders.get(logId.asLong());
            if (builder == null)
            {
                builder = new CoordinatorSummary.Builder(logId);
                builders.put(logId.asLong(), builder);
            }

            return builder;
        }

        public LoggedMutationSummary build()
        {
            int i=0;
            CoordinatorSummary[] summaries = new CoordinatorSummary[builders.size()];
            for (CoordinatorSummary.Builder builder : builders.values())
                summaries[i++] = builder.build();
            Arrays.sort(summaries, CoordinatorSummary.idComparator);
            return new LoggedMutationSummary(tableId, summaries);
        }
    }

    private final TableId tableId;
    private final CoordinatorSummary[] summaries;

    private LoggedMutationSummary(TableId tableId, CoordinatorSummary[] summaries)
    {
        if (summaries.length > 1)
        {
            // validate order
            long lastId = summaries[0].logId.asLong();
            for (int i=1; i < summaries.length; i++)
            {
                long thisId = summaries[i].logId.asLong();
                if (thisId <=lastId)
                    throw new IllegalArgumentException("duplicated or unsorted log id found");
                lastId = thisId;
            }
        }
        this.tableId = tableId;
        this.summaries = summaries;
    }

    @Override
    public TableId tableId()
    {
        return tableId;
    }

    @Override
    public byte[] digest()
    {
        Digest digest = Digest.forReadResponse();
        digest.updateWithLong(tableId.asUUID().getMostSignificantBits());
        digest.updateWithLong(tableId.asUUID().getLeastSignificantBits());
        digest.updateWithInt(summaries.length);

        for (CoordinatorSummary summary : summaries)
            summary.digest(digest);

        return digest.digest();
    }

    int size()
    {
        return summaries.length;
    }

    CoordinatorSummary get(int i)
    {
        return summaries[i];
    }

    public static final IVersionedSerializer<LoggedMutationSummary> serializer = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(LoggedMutationSummary summary, DataOutputPlus out, int version) throws IOException
        {
            summary.tableId.serialize(out);
            out.writeInt(summary.summaries.length);
            for (CoordinatorSummary coordinatorSummary : summary.summaries)
                CoordinatorSummary.serializer.serialize(coordinatorSummary, out, version);
        }

        @Override
        public LoggedMutationSummary deserialize(DataInputPlus in, int version) throws IOException
        {
            TableId tableId = TableId.deserialize(in);
            int size = in.readInt();
            CoordinatorSummary[] summaries = new CoordinatorSummary[size];
            for (int i = 0; i < summaries.length; i++)
                summaries[i] = CoordinatorSummary.serializer.deserialize(in, version);

            return new LoggedMutationSummary(tableId, summaries);
        }

        @Override
        public long serializedSize(LoggedMutationSummary summary, int version)
        {
            long size = summary.tableId.serializedSize();
            size += TypeSizes.sizeof(summary.summaries.length);
            for (CoordinatorSummary coordinatorSummary : summary.summaries)
                size += CoordinatorSummary.serializer.serializedSize(coordinatorSummary, version);
            return size;
        }
    };
}
