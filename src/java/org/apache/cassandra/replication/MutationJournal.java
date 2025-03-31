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
    public static final MutationJournal instance = new MutationJournal();

    private final Journal<ShortMutationId, Mutation> journal;

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

    public RecordPointer write(ShortMutationId id, Mutation mutation)
    {
        return journal.blockingWrite(id, mutation);
    }

    @Nullable
    public Mutation read(ShortMutationId id)
    {
        return journal.readLast(id);
    }

    public boolean read(ShortMutationId id, RecordConsumer<ShortMutationId> consumer)
    {
        return journal.readLast(id, consumer);
    }

    public void readAll(Iterable<ShortMutationId> ids, Collection<Mutation> into)
    {
        for (ShortMutationId id : ids)
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

    static class MutationIdSupport implements KeySupport<ShortMutationId>
    {
        static final int LOG_ID_OFFSET = 0;
        static final int OFFSET_OFFSET = LOG_ID_OFFSET + TypeSizes.LONG_SIZE;

        @Override
        public int serializedSize(int userVersion)
        {
            return TypeSizes.LONG_SIZE  // logId
                 + TypeSizes.INT_SIZE; // offset
        }

        @Override
        public void serialize(ShortMutationId id, DataOutputPlus out, int userVersion) throws IOException
        {
            out.writeLong(id.logId());
            out.writeInt(id.offset());
        }

        @Override
        public void serialize(ShortMutationId id, ByteBuffer out, int userVersion) throws IOException
        {
            out.putLong(id.logId());
            out.putInt(id.offset());
        }

        @Override
        public ShortMutationId deserialize(DataInputPlus in, int userVersion) throws IOException
        {
            long logId = in.readLong();
            int offset = in.readInt();
            return new ShortMutationId(logId, offset);
        }

        @Override
        public ShortMutationId deserialize(ByteBuffer buffer, int position, int userVersion)
        {
            long logId = buffer.getLong(position + LOG_ID_OFFSET);
            int offset = buffer.getInt(position + OFFSET_OFFSET);
            return new ShortMutationId(logId, offset);
        }

        @Override
        public ShortMutationId deserialize(ByteBuffer buffer, int userVersion)
        {
            long logId = buffer.getLong();
            int offset = buffer.getInt();
            return new ShortMutationId(logId, offset);
        }

        @Override
        public void updateChecksum(Checksum crc, ShortMutationId id, int userVersion)
        {
            FBUtilities.updateChecksumLong(crc, id.logId());
            FBUtilities.updateChecksumInt(crc, id.offset());
        }

        @Override
        public int compareWithKeyAt(ShortMutationId id, ByteBuffer buffer, int position, int userVersion)
        {
            int cmp = Long.compare(id.logId(), buffer.getLong(position + LOG_ID_OFFSET));
            return cmp != 0 ? cmp : Integer.compare(id.offset(), buffer.getInt(position + OFFSET_OFFSET));
        }

        @Override
        public int compare(ShortMutationId id1, ShortMutationId id2)
        {
            int cmp = Long.compare(id1.logId(), id2.logId());
            return cmp != 0 ? cmp : Integer.compare(id1.offset(), id2.offset());
        }
    }

    static class MutationSerializer implements ValueSerializer<ShortMutationId, Mutation>
    {
        @Override
        public void serialize(ShortMutationId id, Mutation mutation, DataOutputPlus out, int userVersion) throws IOException
        {
            Mutation.serializer.serialize(mutation, out, userVersion);
        }

        @Override
        public Mutation deserialize(ShortMutationId id, DataInputPlus in, int userVersion) throws IOException
        {
            return Mutation.serializer.deserialize(in, userVersion);
        }
    }
}
