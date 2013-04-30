package org.apache.cassandra.service.paxos;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.UUID;
import java.nio.ByteBuffer;

import com.google.common.base.Objects;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.EmptyColumns;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.io.IVersionedSerializer;
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
        return new Commit(key, ballot, EmptyColumns.factory.create(metadata));
    }

    public static Commit newProposal(ByteBuffer key, UUID ballot, ColumnFamily update)
    {
        return new Commit(key, ballot, updatesWithPaxosTime(update, ballot));
    }

    public static Commit emptyCommit(ByteBuffer key, CFMetaData metadata)
    {
        return new Commit(key, UUIDGen.minTimeUUID(0), EmptyColumns.factory.create(metadata));
    }

    public boolean isAfter(Commit other)
    {
        return ballot.timestamp() > other.ballot.timestamp();
    }

    public boolean hasBallot(UUID ballot)
    {
        return this.ballot.equals(ballot);
    }

    public RowMutation makeMutation()
    {
        assert update != null;
        return new RowMutation(key, update);
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
        for (Column column : updates)
            cf.addAtom(column.withUpdatedTimestamp(t));
        return cf;
    }

    @Override
    public String toString()
    {
        return String.format("Commit(%s, %s, %s)", ByteBufferUtil.bytesToHex(key), ballot, update);
    }

    public static class CommitSerializer implements IVersionedSerializer<Commit>
    {
        public void serialize(Commit commit, DataOutput out, int version) throws IOException
        {
            ByteBufferUtil.writeWithShortLength(commit.key, out);
            UUIDSerializer.serializer.serialize(commit.ballot, out, version);
            ColumnFamily.serializer.serialize(commit.update, out, version);
        }

        public Commit deserialize(DataInput in, int version) throws IOException
        {
            return new Commit(ByteBufferUtil.readWithShortLength(in),
                              UUIDSerializer.serializer.deserialize(in, version),
                              ColumnFamily.serializer.deserialize(in, version));
        }

        public long serializedSize(Commit commit, int version)
        {
            return 2 + commit.key.remaining()
                   + UUIDSerializer.serializer.serializedSize(commit.ballot, version)
                   + ColumnFamily.serializer.serializedSize(commit.update, version);
        }
    }
}
