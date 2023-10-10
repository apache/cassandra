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
import java.util.function.BiFunction;

import javax.annotation.Nullable;

import com.google.common.base.Objects;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableMetadata;

import static org.apache.cassandra.db.SystemKeyspace.*;
import static org.apache.cassandra.service.paxos.Commit.CompareResult.AFTER;
import static org.apache.cassandra.service.paxos.Commit.CompareResult.BEFORE;
import static org.apache.cassandra.service.paxos.Commit.CompareResult.IS_REPROPOSAL;
import static org.apache.cassandra.service.paxos.Commit.CompareResult.WAS_REPROPOSED_BY;
import static org.apache.cassandra.service.paxos.Commit.CompareResult.SAME;
import static org.apache.cassandra.utils.FBUtilities.nowInSeconds;

public class Commit
{
    enum CompareResult { SAME, BEFORE, AFTER, IS_REPROPOSAL, WAS_REPROPOSED_BY}

    public static final CommitSerializer<Commit> serializer = new CommitSerializer<>(Commit::new);

    public static class Proposal extends Commit
    {
        public static final CommitSerializer<Proposal> serializer = new CommitSerializer<>(Proposal::new);

        public Proposal(Ballot ballot, PartitionUpdate update)
        {
            super(ballot, update);
        }

        public String toString()
        {
            return toString("Proposal");
        }

        public static Proposal of(Ballot ballot, PartitionUpdate update)
        {
            update = withTimestamp(update, ballot.unixMicros());
            return new Proposal(ballot, update);
        }

        public static Proposal empty(Ballot ballot, DecoratedKey partitionKey, TableMetadata metadata)
        {
            return new Proposal(ballot, PartitionUpdate.emptyUpdate(metadata, partitionKey));
        }

        public Accepted accepted()
        {
            return new Accepted(ballot, update);
        }

        public Agreed agreed()
        {
            return new Agreed(ballot, update);
        }
    }

    public static class Accepted extends Proposal
    {
        public static final CommitSerializer<Accepted> serializer = new CommitSerializer<>(Accepted::new);

        public static Accepted none(DecoratedKey partitionKey, TableMetadata metadata)
        {
            return new Accepted(Ballot.none(), PartitionUpdate.emptyUpdate(metadata, partitionKey));
        }

        public Accepted(Ballot ballot, PartitionUpdate update)
        {
            super(ballot, update);
        }

        public Accepted(Commit commit)
        {
            super(commit.ballot, commit.update);
        }

        Committed committed()
        {
            return new Committed(ballot, update);
        }

        boolean isExpired(long nowInSec)
        {
            return false;
        }

        public String toString()
        {
            return toString("Accepted");
        }

        /**
         * Like {@link #latest(Commit, Commit)} but also takes into account deletion time
         */
        public static Accepted latestAccepted(Accepted a, Accepted b)
        {
            int c = compare(a, b);
            if (c != 0)
                return c > 0 ? a : b;
            return a instanceof AcceptedWithTTL ? ((AcceptedWithTTL)a).lastDeleted(b) : a;
        }
    }

    public static class AcceptedWithTTL extends Accepted
    {
        public static AcceptedWithTTL withDefaultTTL(Commit copy)
        {
            return new AcceptedWithTTL(copy, nowInSeconds() + legacyPaxosTtlSec(copy.update.metadata()));
        }

        public final long localDeletionTime;

        public AcceptedWithTTL(Commit copy, long localDeletionTime)
        {
            super(copy);
            this.localDeletionTime = localDeletionTime;
        }

        public AcceptedWithTTL(Ballot ballot, PartitionUpdate update, long localDeletionTime)
        {
            super(ballot, update);
            this.localDeletionTime = localDeletionTime;
        }

        boolean isExpired(long nowInSec)
        {
            return nowInSec >= localDeletionTime;
        }

        Accepted lastDeleted(Accepted b)
        {
            return b instanceof AcceptedWithTTL && localDeletionTime >= ((AcceptedWithTTL) b).localDeletionTime
                   ? this : b;
        }
    }

    // might prefer to call this Commit, but would mean refactoring more legacy code
    public static class Agreed extends Accepted
    {
        public static final CommitSerializer<Agreed> serializer = new CommitSerializer<>(Agreed::new);

        public Agreed(Ballot ballot, PartitionUpdate update)
        {
            super(ballot, update);
        }

        public Agreed(Commit copy)
        {
            super(copy);
        }
    }

    public static class Committed extends Agreed
    {
        public static final CommitSerializer<Committed> serializer = new CommitSerializer<>(Committed::new);

        public static Committed none(DecoratedKey partitionKey, TableMetadata metadata)
        {
            return new Committed(Ballot.none(), PartitionUpdate.emptyUpdate(metadata, partitionKey));
        }

        public Committed(Ballot ballot, PartitionUpdate update)
        {
            super(ballot, update);
        }

        public Committed(Commit copy)
        {
            super(copy);
        }

        public String toString()
        {
            return toString("Committed");
        }

        public static Committed latestCommitted(Committed a, Committed b)
        {
            int c = compare(a, b);
            if (c != 0)
                return c > 0 ? a : b;
            return a instanceof CommittedWithTTL ? ((CommittedWithTTL)a).lastDeleted(b) : a;
        }
    }

    public static class CommittedWithTTL extends Committed
    {
        public static CommittedWithTTL withDefaultTTL(Commit copy)
        {
            return new CommittedWithTTL(copy, nowInSeconds() + legacyPaxosTtlSec(copy.update.metadata()));
        }

        public final long localDeletionTime;

        public CommittedWithTTL(Ballot ballot, PartitionUpdate update, long localDeletionTime)
        {
            super(ballot, update);
            this.localDeletionTime = localDeletionTime;
        }

        public CommittedWithTTL(Commit copy, long localDeletionTime)
        {
            super(copy);
            this.localDeletionTime = localDeletionTime;
        }

        boolean isExpired(long nowInSec)
        {
            return nowInSec >= localDeletionTime;
        }

        Committed lastDeleted(Committed b)
        {
            return b instanceof CommittedWithTTL && localDeletionTime >= ((CommittedWithTTL) b).localDeletionTime
                   ? this : b;
        }
    }

    public final Ballot ballot;
    public final PartitionUpdate update;

    public Commit(Ballot ballot, PartitionUpdate update)
    {
        assert ballot != null;
        assert update != null;

        this.ballot = ballot;
        this.update = update;
    }

    public static Commit newPrepare(DecoratedKey partitionKey, TableMetadata metadata, Ballot ballot)
    {
        return new Commit(ballot, PartitionUpdate.emptyUpdate(metadata, partitionKey));
    }

    public static Commit emptyCommit(DecoratedKey partitionKey, TableMetadata metadata)
    {
        return new Commit(Ballot.none(), PartitionUpdate.emptyUpdate(metadata, partitionKey));
    }

    /** @deprecated See CASSANDRA-17164 */
    @Deprecated(since = "4.1")
    public static Commit newProposal(Ballot ballot, PartitionUpdate update)
    {
        update = withTimestamp(update, ballot.unixMicros());
        return new Commit(ballot, update);
    }

    public boolean isAfter(Commit other)
    {
        return other == null || ballot.uuidTimestamp() > other.ballot.uuidTimestamp();
    }

    public boolean isSameOrAfter(@Nullable Ballot otherBallot)
    {
        return otherBallot == null || otherBallot.equals(ballot) || ballot.uuidTimestamp() > otherBallot.uuidTimestamp();
    }

    public boolean isAfter(@Nullable Ballot otherBallot)
    {
        return otherBallot == null || ballot.uuidTimestamp() > otherBallot.uuidTimestamp();
    }

    public boolean isBefore(@Nullable Ballot otherBallot)
    {
        return otherBallot != null && ballot.uuidTimestamp() < otherBallot.uuidTimestamp();
    }

    public boolean hasBallot(Ballot ballot)
    {
        return this.ballot.equals(ballot);
    }

    public boolean hasSameBallot(Commit other)
    {
        return this.ballot.equals(other.ballot);
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
        return toString("Commit");
    }

    public String toString(String kind)
    {
        return String.format("%s(%d:%s, %d:%s)", kind, ballot.uuidTimestamp(), ballot, update.stats().minTimestamp, update.toString(false));
    }

    /**
     * We can witness reproposals of the latest successful commit; we can detect this by comparing the timestamp of
     * the update with our ballot; if it is the same, we are not a reproposal. If it is the same as either the
     * ballot timestamp or update timestamp of the latest committed proposal, then we are reproposing it and can
     * instead simpy commit it.
     */
    public boolean isReproposalOf(Commit older)
    {
        return isReproposal(older, older.ballot.uuidTimestamp(), this, this.ballot.uuidTimestamp());
    }

    private boolean isReproposal(Commit older, long ballotOfOlder, Commit newer, long ballotOfNewer)
    {
        // NOTE: it would in theory be possible to just check
        // newer.update.stats().minTimestamp == older.update.stats().minTimestamp
        // however this could be brittle, if for some reason they don't get updated;
        // the logic below is fail-safe, in that if minTimestamp is not set we will treat it as not a reproposal
        // which is the safer way to get it wrong.

        // the timestamp of a mutation stays unchanged as we repropose it, so the timestamp of the mutation
        // is the timestamp of the ballot that originally proposed it
        long originalBallotOfNewer = newer.update.stats().minTimestamp;

        // so, if the mutation and ballot timestamps match, this is not a reproposal but a first proposal
        if (ballotOfNewer == originalBallotOfNewer)
            return false;

        // otherwise, if the original proposing ballot matches the older proposal's ballot, it is reproposing it
        if (originalBallotOfNewer == ballotOfOlder)
            return true;

        // otherwise, it could be that both are reproposals, so just check both for the "original" ballot timestamp
        return originalBallotOfNewer == older.update.stats().minTimestamp;
    }

    public CompareResult compareWith(Commit that)
    {
        long thisBallot = this.ballot.uuidTimestamp();
        long thatBallot = that.ballot.uuidTimestamp();
        // by the time we reach proposal and commit, timestamps are unique so we can assert identity
        if (thisBallot == thatBallot)
            return SAME;

        if (thisBallot < thatBallot)
            return isReproposal(this, thisBallot, that, thatBallot) ? WAS_REPROPOSED_BY : BEFORE;
        else
            return isReproposal(that, thatBallot, this, thisBallot) ? IS_REPROPOSAL : AFTER;
    }

    private static int compare(@Nullable Commit a, @Nullable Commit b)
    {
        if (a == null) return 1;
        if (b == null) return -1;
        return Long.compare(a.ballot.uuidTimestamp(), b.ballot.uuidTimestamp());
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
    public static boolean isAfter(@Nullable Ballot testIsAfter, @Nullable Commit testIsBefore)
    {
        return testIsAfter != null && (testIsBefore == null || testIsAfter.uuidTimestamp() > testIsBefore.ballot.uuidTimestamp());
    }

    /**
     * @return testIfAfter.isAfter(testIfBefore), with non-null > null
     */
    public static boolean isAfter(@Nullable Commit testIsAfter, @Nullable Ballot testIsBefore)
    {
        return testIsAfter != null && (testIsBefore == null || testIsAfter.ballot.uuidTimestamp() > testIsBefore.uuidTimestamp());
    }

    /**
     * @return testIfAfter.isAfter(testIfBefore), with non-null > null
     */
    public static boolean isAfter(@Nullable Ballot testIsAfter, @Nullable Ballot testIsBefore)
    {
        return testIsAfter != null && (testIsBefore == null || testIsAfter.uuidTimestamp() > testIsBefore.uuidTimestamp());
    }

    /**
     * the latest of two ballots, or the first ballot if equal timestamps
     */
    public static <C extends Commit> C latest(@Nullable C a, @Nullable C b)
    {
        return (a == null | b == null) ? (a == null ? b : a) : a.ballot.uuidTimestamp() >= b.ballot.uuidTimestamp() ? a : b;
    }

    /**
     * the latest of two ballots, or the first ballot if equal timestamps
     */
    public static Ballot latest(@Nullable Commit a, @Nullable Ballot b)
    {
        return (a == null | b == null) ? (a == null ? b : a.ballot) : a.ballot.uuidTimestamp() >= b.uuidTimestamp() ? a.ballot : b;
    }

    /**
     * the latest of two ballots, or the first ballot if equal timestamps
     */
    public static Ballot latest(@Nullable Ballot a, @Nullable Ballot b)
    {
        return (a == null | b == null) ? (a == null ? b : a) : a.uuidTimestamp() >= b.uuidTimestamp() ? a : b;
    }

    /**
     * unequal ballots with same timestamp
     */
    public static boolean timestampsClash(@Nullable Commit a, @Nullable Ballot b)
    {
        return a != null && b != null && !a.ballot.equals(b) && a.ballot.uuidTimestamp() == b.uuidTimestamp();
    }

    public static boolean timestampsClash(@Nullable Ballot a, @Nullable Ballot b)
    {
        return a != null && b != null && !a.equals(b) && a.uuidTimestamp() == b.uuidTimestamp();
    }

    private static PartitionUpdate withTimestamp(PartitionUpdate update, long timestamp)
    {
        return new PartitionUpdate.Builder(update, 0).updateAllTimestamp(timestamp).build();
    }

    public static class CommitSerializer<T extends Commit> implements IVersionedSerializer<T>
    {
        final BiFunction<Ballot, PartitionUpdate, T> constructor;
        public CommitSerializer(BiFunction<Ballot, PartitionUpdate, T> constructor)
        {
            this.constructor = constructor;
        }

        public void serialize(T commit, DataOutputPlus out, int version) throws IOException
        {
            commit.ballot.serialize(out);
            PartitionUpdate.serializer.serialize(commit.update, out, version);
        }

        public T deserialize(DataInputPlus in, int version) throws IOException
        {
            Ballot ballot = Ballot.deserialize(in);
            PartitionUpdate update = PartitionUpdate.serializer.deserialize(in, version, DeserializationHelper.Flag.LOCAL);
            return constructor.apply(ballot, update);
        }

        public long serializedSize(T commit, int version)
        {
            return Ballot.sizeInBytes()
                   + PartitionUpdate.serializer.serializedSize(commit.update, version);
        }
    }

}
