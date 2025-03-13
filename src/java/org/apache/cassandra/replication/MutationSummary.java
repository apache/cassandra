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

package org.apache.cassandra.replication;

import com.google.common.base.Preconditions;
import org.agrona.collections.Long2ObjectHashMap;
import org.apache.cassandra.db.Digest;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableId;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;

public class MutationSummary
{
    public static class CoordinatorSummary
    {
        private static final Comparator<CoordinatorSummary> idComparator = Comparator.comparing(o -> o.logId());

        public final Offsets reconciled;
        public final Offsets unreconciled;

        public CoordinatorSummary(Offsets reconciled, Offsets unreconciled)
        {
            Preconditions.checkArgument(reconciled.logId().equals(unreconciled.logId()));
            this.reconciled = reconciled;
            this.unreconciled = unreconciled;
        }

        @Override
        public boolean equals(Object o)
        {
            if (o == null || getClass() != o.getClass()) return false;
            CoordinatorSummary summary = (CoordinatorSummary) o;
            return Objects.equals(reconciled, summary.reconciled) && Objects.equals(unreconciled, summary.unreconciled);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(reconciled, unreconciled);
        }

        @Override
        public String toString()
        {
            return "CoordinatorSummary{" +
                    "logId=" + logId() +
                    ", reconciled=" + reconciled +
                    ", unreconciled=" + unreconciled +
                    '}';
        }

        public CoordinatorLogId logId()
        {
            return reconciled.logId();
        }

        public static Comparator<CoordinatorSummary> idComparator()
        {
            return idComparator;
        }

        boolean contains(int offset)
        {
            return reconciled.contains(offset) || unreconciled.contains(offset);
        }

        void digest(Digest digest)
        {
            reconciled.digest(digest);
            unreconciled.digest(digest);
        }

        public static class Builder
        {
            public final CoordinatorLogId logId;
            public final Offsets reconciled;
            public final Offsets unreconciled;

            public Builder(CoordinatorLogId logId)
            {
                this.logId = logId;
                reconciled = new Offsets(logId);
                unreconciled = new Offsets(logId);
            }

            public CoordinatorSummary build()
            {
                return new CoordinatorSummary(reconciled, unreconciled);
            }
        }

        public static final IVersionedSerializer<CoordinatorSummary> serializer = new IVersionedSerializer<>()
        {
            @Override
            public void serialize(CoordinatorSummary t, DataOutputPlus out, int version) throws IOException
            {
                Offsets.serializer.serialize(t.reconciled, out, version);
                Offsets.serializer.serialize(t.unreconciled, out, version);
            }

            @Override
            public CoordinatorSummary deserialize(DataInputPlus in, int version) throws IOException
            {
                return new CoordinatorSummary(Offsets.serializer.deserialize(in, version),
                                              Offsets.serializer.deserialize(in, version));
            }

            @Override
            public long serializedSize(CoordinatorSummary t, int version)
            {
                return Offsets.serializer.serializedSize(t.reconciled, version)
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

        public MutationSummary build()
        {
            int i=0;
            CoordinatorSummary[] summaries = new CoordinatorSummary[builders.size()];
            for (CoordinatorSummary.Builder builder : builders.values())
                summaries[i++] = builder.build();
            Arrays.sort(summaries, CoordinatorSummary.idComparator);
            return new MutationSummary(tableId, summaries);
        }
    }

    private final TableId tableId;
    private final CoordinatorSummary[] summaries;
    private transient final Long2ObjectHashMap<CoordinatorSummary> coordinatorSummaryMap = new Long2ObjectHashMap<>();

    private MutationSummary(TableId tableId, CoordinatorSummary[] summaries)
    {
        long lastId = 0;
        for (int i=0; i<summaries.length; i++)
        {
            long thisId = summaries[i].logId().asLong();
            if (i > 0 && thisId <= lastId)
                throw new IllegalArgumentException("duplicated or unsorted log id found");

            coordinatorSummaryMap.put(thisId, summaries[i]);
            lastId = thisId;
        }

        this.tableId = tableId;
        this.summaries = summaries;
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || getClass() != o.getClass()) return false;
        MutationSummary summary = (MutationSummary) o;
        return Objects.equals(tableId, summary.tableId) && Objects.deepEquals(summaries, summary.summaries);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tableId, Arrays.hashCode(summaries));
    }

    @Override
    public String toString()
    {
        return "MutationSummary{" +
                "tableId=" + tableId +
                ", summaries=" + Arrays.toString(summaries) +
                '}';
    }

    public TableId tableId()
    {
        return tableId;
    }

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

    public boolean contains(MutationId id)
    {
        CoordinatorSummary summary = coordinatorSummaryMap.get(id.logId);
        return summary != null && summary.contains(id.offset());
    }

    public int unreconciledIds()
    {
        int count = 0;
        for (CoordinatorSummary summary : summaries)
            count += summary.unreconciled.offsetCount();
        return count;
    }

    public int size()
    {
        return summaries.length;
    }

    boolean isEmpty()
    {
        return size() == 0;
    }

    public CoordinatorSummary get(int i)
    {
        return summaries[i];
    }

    public CoordinatorSummary get(CoordinatorLogId logId)
    {
        return coordinatorSummaryMap.get(logId.asLong());
    }

    public static final IVersionedSerializer<MutationSummary> serializer = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(MutationSummary summary, DataOutputPlus out, int version) throws IOException
        {
            summary.tableId.serialize(out);
            out.writeInt(summary.summaries.length);
            for (CoordinatorSummary coordinatorSummary : summary.summaries)
                CoordinatorSummary.serializer.serialize(coordinatorSummary, out, version);
        }

        @Override
        public MutationSummary deserialize(DataInputPlus in, int version) throws IOException
        {
            TableId tableId = TableId.deserialize(in);
            int size = in.readInt();
            CoordinatorSummary[] summaries = new CoordinatorSummary[size];
            for (int i = 0; i < summaries.length; i++)
                summaries[i] = CoordinatorSummary.serializer.deserialize(in, version);

            return new MutationSummary(tableId, summaries);
        }

        @Override
        public long serializedSize(MutationSummary summary, int version)
        {
            long size = summary.tableId.serializedSize();
            size += TypeSizes.sizeof(summary.summaries.length);
            for (CoordinatorSummary coordinatorSummary : summary.summaries)
                size += CoordinatorSummary.serializer.serializedSize(coordinatorSummary, version);
            return size;
        }
    };
}
