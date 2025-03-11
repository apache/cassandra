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

import org.agrona.collections.Long2ObjectHashMap;
import org.apache.cassandra.db.Digest;
import org.apache.cassandra.replication.MutationSummary;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.tracking.CoordinatorLogId;
import org.apache.cassandra.service.tracking.SequenceIds;

import java.util.Arrays;
import java.util.Comparator;

public class LoggedMutationSummary implements MutationSummary
{
    public static class CoordinatorSummary
    {
        private static final Comparator<CoordinatorSummary> idComparator = (o1, o2) -> o1.logId.compareTo(o2.logId);
        public final CoordinatorLogId logId;
        public final SequenceIds reconciledIds;
        public final SequenceIds unreconciledIds;

        public CoordinatorSummary(CoordinatorLogId logId, SequenceIds reconciledIds, SequenceIds unreconciledIds)
        {
            this.logId = logId;
            this.reconciledIds = reconciledIds;
            this.unreconciledIds = unreconciledIds;
        }

        void digest(Digest digest)
        {
            digest.updateWithLong(logId.asLong());
            reconciledIds.digest(digest);
            unreconciledIds.digest(digest);
        }

        public static class Builder
        {
            public final CoordinatorLogId logId;
            public final SequenceIds reconciledIds = new SequenceIds();
            public final SequenceIds unreconciledIds = new SequenceIds();

            public Builder(CoordinatorLogId logId)
            {
                this.logId = logId;
            }

            public CoordinatorSummary build()
            {
                return new CoordinatorSummary(logId, reconciledIds, unreconciledIds);
            }
        }
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
}
