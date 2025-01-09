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
package org.apache.cassandra.service.tracking;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.zip.Checksum;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.journal.Journal;
import org.apache.cassandra.journal.KeySupport;
import org.apache.cassandra.journal.Params;
import org.apache.cassandra.journal.RecordConsumer;
import org.apache.cassandra.journal.RecordPointer;
import org.apache.cassandra.journal.SegmentCompactor;
import org.apache.cassandra.journal.ValueSerializer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.FBUtilities;

public class MutationJournal
{
    private final Journal<MutationId, Mutation> journal;

    private MutationJournal()
    {
        this(new File(DatabaseDescriptor.getCommitLogLocation()), new JournalParams());
    }

    @VisibleForTesting
    MutationJournal(File directory, Params params)
    {
        journal = new Journal<>("MutationJournal", directory, params, new MutationIdSupport(), new MutationSerializer(), SegmentCompactor.noop());
    }

    public void start()
    {
        journal.start();
    }

    public void shutdownBlocking()
    {
        journal.shutdown();
    }

    public RecordPointer write(MutationId id, Mutation mutation)
    {
        return journal.blockingWrite(id, mutation);
    }

    @Nullable
    public Mutation read(MutationId id)
    {
        return journal.readLast(id);
    }

    public boolean read(MutationId id, RecordConsumer<MutationId> consumer)
    {
        return journal.readLast(id, consumer);
    }

    public void readAll(Iterable<MutationId> ids, Collection<Mutation> into)
    {
        for (MutationId id : ids)
        {
            Mutation mutation = read(id);
            Preconditions.checkState(mutation != null);
            into.add(mutation);
        }
    }

    static class JournalParams implements Params
    {
        @Override
        public int segmentSize()
        {
            return DatabaseDescriptor.getCommitLogSegmentSize();
        }

        @Override
        public FailurePolicy failurePolicy()
        {
            return FailurePolicy.STOP;
        }

        @Override
        public FlushMode flushMode()
        {
            return FlushMode.PERIODIC;
        }

        @Override
        public boolean enableCompaction()
        {
            return false;
        }

        @Override
        public long compactionPeriod(TimeUnit units)
        {
            return 0;
        }

        @Override
        public long flushPeriod(TimeUnit units)
        {
            return units.convert(DatabaseDescriptor.getCommitLogSyncPeriod(), TimeUnit.MILLISECONDS);
        }

        @Override
        public long periodicBlockPeriod(TimeUnit units)
        {
            return units.convert(DatabaseDescriptor.getPeriodicCommitLogSyncBlock(), TimeUnit.MILLISECONDS);
        }

        @Override
        public int userVersion()
        {
            return MessagingService.current_version;
        }
    }

    static class MutationIdSupport implements KeySupport<MutationId>
    {
        static final int LOG_ID_OFFSET = 0;
        static final int SEQUENCE_ID_OFFSET = LOG_ID_OFFSET + TypeSizes.LONG_SIZE;

        @Override
        public int serializedSize(int userVersion)
        {
            return TypeSizes.LONG_SIZE  // logId
                 + TypeSizes.LONG_SIZE; // sequenceId
        }

        @Override
        public void serialize(MutationId id, DataOutputPlus out, int userVersion) throws IOException
        {
            out.writeLong(id.logId);
            out.writeLong(id.sequenceId);
        }

        @Override
        public void serialize(MutationId id, ByteBuffer out, int userVersion) throws IOException
        {
            out.putLong(id.logId);
            out.putLong(id.sequenceId);
        }

        @Override
        public MutationId deserialize(DataInputPlus in, int userVersion) throws IOException
        {
            long logId = in.readLong();
            long sequenceId = in.readLong();
            return new MutationId(logId, sequenceId);
        }

        @Override
        public MutationId deserialize(ByteBuffer buffer, int position, int userVersion)
        {
            long logId = buffer.getLong(position + LOG_ID_OFFSET);
            long sequenceId = buffer.getLong(position + SEQUENCE_ID_OFFSET);
            return new MutationId(logId, sequenceId);
        }

        @Override
        public MutationId deserialize(ByteBuffer buffer, int userVersion)
        {
            long logId = buffer.getLong();
            long sequenceId = buffer.getLong();
            return new MutationId(logId, sequenceId);
        }

        @Override
        public void updateChecksum(Checksum crc, MutationId id, int userVersion)
        {
            FBUtilities.updateChecksumLong(crc, id.logId);
            FBUtilities.updateChecksumLong(crc, id.sequenceId);
        }

        @Override
        public int compareWithKeyAt(MutationId id, ByteBuffer buffer, int position, int userVersion)
        {
            int cmp = Long.compare(id.logId, buffer.getLong(position + LOG_ID_OFFSET));
            return cmp != 0 ? cmp : Long.compare(id.sequenceId, buffer.getLong(position + SEQUENCE_ID_OFFSET));
        }

        @Override
        public int compare(MutationId id1, MutationId id2)
        {
            int cmp = Long.compare(id1.logId, id2.logId);
            return cmp != 0 ? cmp : Long.compare(id1.sequenceId, id2.sequenceId);
        }
    }

    static class MutationSerializer implements ValueSerializer<MutationId, Mutation>
    {
        @Override
        public void serialize(MutationId id, Mutation mutation, DataOutputPlus out, int userVersion) throws IOException
        {
            Mutation.serializer.serialize(mutation, out, userVersion);
        }

        @Override
        public Mutation deserialize(MutationId id, DataInputPlus in, int userVersion) throws IOException
        {
            return Mutation.serializer.deserialize(in, userVersion);
        }
    }
}
