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
import java.nio.ByteBuffer;

import com.google.common.base.Objects;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.UUIDSerializer;

public class Commit
{
    public static final CommitSerializer serializer = new CommitSerializer();

    public final ByteBuffer key;
    public final UUID ballot;
    public final ColumnFamily update;

    public Commit(ByteBuffer key, UUID ballot, ColumnFamily update)
    {
        assert key != null;
        assert ballot != null;
        assert update != null;

        this.key = key;
        this.ballot = ballot;
        this.update = update;
    }

    public static Commit newPrepare(ByteBuffer key, CFMetaData metadata, UUID ballot)
    {
        return new Commit(key, ballot, ArrayBackedSortedColumns.factory.create(metadata));
    }

    public static Commit newProposal(ByteBuffer key, UUID ballot, ColumnFamily update)
    {
        return new Commit(key, ballot, updatesWithPaxosTime(update, ballot));
    }

    public static Commit emptyCommit(ByteBuffer key, CFMetaData metadata)
    {
        return new Commit(key, UUIDGen.minTimeUUID(0), ArrayBackedSortedColumns.factory.create(metadata));
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
        return new Mutation(key, update);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Commit commit = (Commit) o;

        if (!ballot.equals(commit.ballot)) return false;
        if (!key.equals(commit.key)) return false;
        if (!update.equals(commit.update)) return false;

        return true;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(key, ballot, update);
    }

    private static ColumnFamily updatesWithPaxosTime(ColumnFamily updates, UUID ballot)
    {
        ColumnFamily cf = updates.cloneMeShallow();
        long t = UUIDGen.microsTimestamp(ballot);
        // For the tombstones, we use t-1 so that when insert a collection literall, the range tombstone that deletes the previous values of
        // the collection and we want that to have a lower timestamp and our new values. Since tombstones wins over normal insert, using t-1
        // should not be a problem in general (see #6069).
        cf.deletionInfo().updateAllTimestamp(t-1);
        for (Cell cell : updates)
            cf.addAtom(cell.withUpdatedTimestamp(t));
        return cf;
    }

    @Override
    public String toString()
    {
        return String.format("Commit(%s, %s, %s)", ByteBufferUtil.bytesToHex(key), ballot, update);
    }

    public static class CommitSerializer implements IVersionedSerializer<Commit>
    {
        public void serialize(Commit commit, DataOutputPlus out, int version) throws IOException
        {
            ByteBufferUtil.writeWithShortLength(commit.key, out);
            UUIDSerializer.serializer.serialize(commit.ballot, out, version);
            ColumnFamily.serializer.serialize(commit.update, out, version);
        }

        public Commit deserialize(DataInput in, int version) throws IOException
        {
            return new Commit(ByteBufferUtil.readWithShortLength(in),
                              UUIDSerializer.serializer.deserialize(in, version),
                              ColumnFamily.serializer.deserialize(in,
                                                                  ArrayBackedSortedColumns.factory,
                                                                  ColumnSerializer.Flag.LOCAL,
                                                                  version));
        }

        public long serializedSize(Commit commit, int version)
        {
            return 2 + commit.key.remaining()
                   + UUIDSerializer.serializer.serializedSize(commit.ballot, version)
                   + ColumnFamily.serializer.serializedSize(commit.update, version);
        }
    }
}
