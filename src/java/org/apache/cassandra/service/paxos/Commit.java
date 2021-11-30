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


import java.io.IOException;
import java.util.UUID;

import javax.annotation.Nullable;

import com.google.common.base.Objects;

import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.UUIDSerializer;

public class Commit
{
    public static final CommitSerializer serializer = new CommitSerializer();

    public final TimeUUID ballot;
    public final PartitionUpdate update;

    public Commit(TimeUUID ballot, PartitionUpdate update)
    {
        assert ballot != null;
        assert update != null;

        this.ballot = ballot;
        this.update = update;
    }

    public static Commit newPrepare(DecoratedKey key, TableMetadata metadata, TimeUUID ballot)
    {
        return new Commit(ballot, PartitionUpdate.emptyUpdate(metadata, key));
    }

    public static Commit newProposal(TimeUUID ballot, PartitionUpdate update)
    {
        PartitionUpdate withNewTimestamp = new PartitionUpdate.Builder(update, 0).updateAllTimestamp(ballot.unixMicros()).build();
        return new Commit(ballot, withNewTimestamp);
    }

    public static Commit emptyCommit(DecoratedKey key, TableMetadata metadata)
    {
        return new Commit(TimeUUID.minAtUnixMillis(0), PartitionUpdate.emptyUpdate(metadata, key));
    }

    public boolean isAfter(Commit other)
    {
        return ballot.rawTimestamp() > other.ballot.rawTimestamp();
    }

    public boolean hasBallot(TimeUUID ballot)
    {
        return this.ballot.equals(ballot);
    }

    /** Whether this is an empty commit, that is one with no updates. */
    public boolean isEmpty()
    {
        return update.isEmpty();
    }

    public Mutation makeMutation()
    {
        return new Mutation(update);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Commit commit = (Commit) o;

        return ballot.equals(commit.ballot) && update.equals(commit.update);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(ballot, update);
    }

    @Override
    public String toString()
    {
        return String.format("Commit(%s, %s)", ballot, update);
    }

    /**
     * @return testIfAfter.isAfter(testIfBefore), with non-null > null
     */
    public static boolean isAfter(@Nullable Commit testIsAfter, @Nullable Commit testIsBefore)
    {
        return testIsAfter != null && testIsAfter.isAfter(testIsBefore);
    }

    /**
     * @return testIfAfter.isAfter(testIfBefore), with non-null > null
     */
    public static boolean isAfter(@Nullable TimeUUID testIsAfter, @Nullable Commit testIsBefore)
    {
        return testIsAfter != null && (testIsBefore == null || testIsAfter.rawTimestamp() > testIsBefore.ballot.rawTimestamp());
    }

    /**
     * @return testIfAfter.isAfter(testIfBefore), with non-null > null
     */
    public static boolean isAfter(@Nullable Commit testIsAfter, @Nullable TimeUUID testIsBefore)
    {
        return testIsAfter != null && (testIsBefore == null || testIsAfter.ballot.rawTimestamp() > testIsBefore.rawTimestamp());
    }

    /**
     * @return testIfAfter.isAfter(testIfBefore), with non-null > null
     */
    public static boolean isAfter(@Nullable UUID testIsAfter, @Nullable UUID testIsBefore)
    {
        return testIsAfter != null && (testIsBefore == null || testIsAfter.timestamp() > testIsBefore.timestamp());
    }

    public static class CommitSerializer implements IVersionedSerializer<Commit>
    {
        public void serialize(Commit commit, DataOutputPlus out, int version) throws IOException
        {
            commit.ballot.serialize(out);
            PartitionUpdate.serializer.serialize(commit.update, out, version);
        }

        public Commit deserialize(DataInputPlus in, int version) throws IOException
        {
            TimeUUID ballot = TimeUUID.deserialize(in);
            PartitionUpdate update = PartitionUpdate.serializer.deserialize(in, version, DeserializationHelper.Flag.LOCAL);
            return new Commit(ballot, update);
        }

        public long serializedSize(Commit commit, int version)
        {
            return TimeUUID.sizeInBytes()
                 + PartitionUpdate.serializer.serializedSize(commit.update, version);
        }
    }
}
