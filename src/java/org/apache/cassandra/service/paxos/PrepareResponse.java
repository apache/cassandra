package org.apache.cassandra.service.paxos;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDSerializer;

public class PrepareResponse
{
    public static final PrepareResponseSerializer serializer = new PrepareResponseSerializer();

    public final boolean promised;
    public final Commit inProgressCommit;
    public final Commit mostRecentCommit;

    public PrepareResponse(boolean promised, Commit inProgressCommit, Commit mostRecentCommit)
    {
        assert inProgressCommit.key == mostRecentCommit.key;
        assert inProgressCommit.update.metadata() == mostRecentCommit.update.metadata();

        this.promised = promised;
        this.mostRecentCommit = mostRecentCommit;
        this.inProgressCommit = inProgressCommit;
    }

    @Override
    public String toString()
    {
        return String.format("PrepareResponse(%s, %s, %s)", promised, mostRecentCommit, inProgressCommit);
    }

    public static class PrepareResponseSerializer implements IVersionedSerializer<PrepareResponse>
    {
        public void serialize(PrepareResponse response, DataOutput out, int version) throws IOException
        {
            out.writeBoolean(response.promised);
            ByteBufferUtil.writeWithShortLength(response.inProgressCommit.key, out);
            UUIDSerializer.serializer.serialize(response.inProgressCommit.ballot, out, version);
            ColumnFamily.serializer.serialize(response.inProgressCommit.update, out, version);
            UUIDSerializer.serializer.serialize(response.mostRecentCommit.ballot, out, version);
            ColumnFamily.serializer.serialize(response.mostRecentCommit.update, out, version);
        }

        public PrepareResponse deserialize(DataInput in, int version) throws IOException
        {
            boolean success = in.readBoolean();
            ByteBuffer key = ByteBufferUtil.readWithShortLength(in);
            return new PrepareResponse(success,
                                       new Commit(key,
                                                  UUIDSerializer.serializer.deserialize(in, version),
                                                  ColumnFamily.serializer.deserialize(in, version)),
                                       new Commit(key,
                                                  UUIDSerializer.serializer.deserialize(in, version),
                                                  ColumnFamily.serializer.deserialize(in, version)));
        }

        public long serializedSize(PrepareResponse response, int version)
        {
            return 1
                   + 2 + response.inProgressCommit.key.remaining()
                   + UUIDSerializer.serializer.serializedSize(response.inProgressCommit.ballot, version)
                   + ColumnFamily.serializer.serializedSize(response.inProgressCommit.update, version)
                   + UUIDSerializer.serializer.serializedSize(response.mostRecentCommit.ballot, version)
                   + ColumnFamily.serializer.serializedSize(response.mostRecentCommit.update, version);
        }
    }
}
