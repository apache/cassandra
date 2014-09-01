package org.apache.cassandra.service.paxos;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import java.io.DataInput;
import java.io.IOException;
import java.util.UUID;

import com.google.common.base.Objects;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.UUIDSerializer;

public class Commit
{
    public static final CommitSerializer serializer = new CommitSerializer();

    public final UUID ballot;
    public final PartitionUpdate update;

    public Commit(UUID ballot, PartitionUpdate update)
    {
        assert ballot != null;
        assert update != null;

        this.ballot = ballot;
        this.update = update;
    }

    public static Commit newPrepare(DecoratedKey key, CFMetaData metadata, UUID ballot)
    {
        return new Commit(ballot, PartitionUpdate.emptyUpdate(metadata, key));
    }

    public static Commit newProposal(UUID ballot, PartitionUpdate update)
    {
        return new Commit(ballot, updatesWithPaxosTime(update, ballot));
    }

    public static Commit emptyCommit(DecoratedKey key, CFMetaData metadata)
    {
        return new Commit(UUIDGen.minTimeUUID(0), PartitionUpdate.emptyUpdate(metadata, key));
    }

    public boolean isAfter(Commit other)
    {
        return ballot.timestamp() > other.ballot.timestamp();
    }

    public boolean hasBallot(UUID ballot)
    {
        return this.ballot.equals(ballot);
    }

    public Mutation makeMutation()
    {
        assert update != null;
        return new Mutation(update);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Commit commit = (Commit) o;

        if (!ballot.equals(commit.ballot)) return false;
        if (!update.equals(commit.update)) return false;

        return true;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(ballot, update);
    }

    private static PartitionUpdate updatesWithPaxosTime(PartitionUpdate update, UUID ballot)
    {
        long t = UUIDGen.microsTimestamp(ballot);
        // Using t-1 for tombstones so deletion doesn't trump newly inserted data (#6069)
        PartitionUpdate newUpdate = new PartitionUpdate(update.metadata(),
                                                        update.partitionKey(),
                                                        update.deletionInfo().updateAllTimestamp(t-1),
                                                        update.columns(),
                                                        update.rowCount());

        if (!update.staticRow().isEmpty())
            copyWithUpdatedTimestamp(update.staticRow(), newUpdate.staticWriter(), t);

        for (Row row : update)
            copyWithUpdatedTimestamp(row, newUpdate.writer(), t);

        return newUpdate;
    }

    private static void copyWithUpdatedTimestamp(Row row, Row.Writer writer, long timestamp)
    {
        Rows.writeClustering(row.clustering(), writer);
        writer.writePartitionKeyLivenessInfo(row.primaryKeyLivenessInfo().withUpdatedTimestamp(timestamp));
        writer.writeRowDeletion(row.deletion());

        for (Cell cell : row)
            writer.writeCell(cell.column(), cell.isCounterCell(), cell.value(), cell.livenessInfo().withUpdatedTimestamp(timestamp), cell.path());

        for (int i = 0; i < row.columns().complexColumnCount(); i++)
        {
            ColumnDefinition c = row.columns().getComplex(i);
            DeletionTime dt = row.getDeletion(c);
            // We use t-1 to make sure that on inserting a collection literal, the deletion that comes with it does not
            // end up deleting the inserted data (see #6069)
            if (!dt.isLive())
                writer.writeComplexDeletion(c, new SimpleDeletionTime(timestamp-1, dt.localDeletionTime()));
        }
        writer.endOfRow();
    }

    @Override
    public String toString()
    {
        return String.format("Commit(%s, %s)", ballot, update);
    }

    public static class CommitSerializer implements IVersionedSerializer<Commit>
    {
        public void serialize(Commit commit, DataOutputPlus out, int version) throws IOException
        {
            if (version < MessagingService.VERSION_30)
                ByteBufferUtil.writeWithShortLength(commit.update.partitionKey().getKey(), out);

            UUIDSerializer.serializer.serialize(commit.ballot, out, version);
            PartitionUpdate.serializer.serialize(commit.update, out, version);
        }

        public Commit deserialize(DataInput in, int version) throws IOException
        {
            DecoratedKey key = null;
            if (version < MessagingService.VERSION_30)
                key = StorageService.getPartitioner().decorateKey(ByteBufferUtil.readWithShortLength(in));

            UUID ballot = UUIDSerializer.serializer.deserialize(in, version);
            PartitionUpdate update = PartitionUpdate.serializer.deserialize(in, version, SerializationHelper.Flag.LOCAL, key);
            return new Commit(ballot, update);
        }

        public long serializedSize(Commit commit, int version)
        {
            TypeSizes sizes = TypeSizes.NATIVE;

            int size = 0;
            if (version < MessagingService.VERSION_30)
                size += ByteBufferUtil.serializedSizeWithShortLength(commit.update.partitionKey().getKey(), sizes);

            return size
                 + UUIDSerializer.serializer.serializedSize(commit.ballot, version)
                 + PartitionUpdate.serializer.serializedSize(commit.update, version, sizes);
        }
    }
}
