package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;

import java.io.DataInput;
import java.io.IOException;
import java.util.Comparator;

public class ExecutionInfo implements Comparable<ExecutionInfo>
{
    public final long epoch;
    public final long executed;

    public ExecutionInfo(long epoch, long executed)
    {
        this.epoch = epoch;
        this.executed = executed;
    }

    public static final Comparator<ExecutionInfo> comparator = new Comparator<ExecutionInfo>()
    {
        @Override
        public int compare(ExecutionInfo o1, ExecutionInfo o2)
        {
            if (o1.epoch != o2.epoch)
            {
                return Long.valueOf(o1.epoch).compareTo(o2.epoch);
            }
            return Long.valueOf(o1.executed).compareTo(o2.executed);
        }
    };

    @Override
    public int compareTo(ExecutionInfo o)
    {
        return comparator.compare(this, o);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ExecutionInfo info = (ExecutionInfo) o;

        if (epoch != info.epoch) return false;
        if (executed != info.executed) return false;

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (epoch ^ (epoch >>> 32));
        result = 31 * result + (int) (executed ^ (executed >>> 32));
        return result;
    }

    @Override
    public String toString()
    {
        return "ExecutionInfo{" +
                "epoch=" + epoch +
                ", executed=" + executed +
                '}';
    }

    public static final IVersionedSerializer<ExecutionInfo> serializer = new IVersionedSerializer<ExecutionInfo>()
    {
        @Override
        public void serialize(ExecutionInfo e, DataOutputPlus out, int version) throws IOException
        {
            out.writeLong(e.epoch);
            out.writeLong(e.executed);
        }

        @Override
        public ExecutionInfo deserialize(DataInput in, int version) throws IOException
        {
            return new ExecutionInfo(in.readLong(), in.readLong());
        }

        @Override
        public long serializedSize(ExecutionInfo e, int version)
        {
            return 8 * 2;
        }
    };

    public static class Tuple <T>
    {
        public final ExecutionInfo executionInfo;
        public final T payload;

        public Tuple(ExecutionInfo executionInfo, T payload)
        {
            this.executionInfo = executionInfo;
            this.payload = payload;
        }

        public static <T> IVersionedSerializer<Tuple<T>> createSerializer(final IVersionedSerializer<T> payloadSerializer)
        {
            return new IVersionedSerializer<Tuple<T>>()
            {
                @Override
                public void serialize(Tuple<T> t, DataOutputPlus out, int version) throws IOException
                {
                    serializer.serialize(t.executionInfo, out, version);
                    payloadSerializer.serialize(t.payload, out, version);
                }

                @Override
                public Tuple<T> deserialize(DataInput in, int version) throws IOException
                {
                    return new Tuple<>(serializer.deserialize(in, version), payloadSerializer.deserialize(in, version));
                }

                @Override
                public long serializedSize(Tuple<T> t, int version)
                {
                    return serializer.serializedSize(t.executionInfo, version) + payloadSerializer.serializedSize(t.payload, version);
                }
            };
        }
    }
}
