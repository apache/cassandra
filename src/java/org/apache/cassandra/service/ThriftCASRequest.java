package org.apache.cassandra.service;

import com.google.common.collect.ImmutableSortedSet;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.db.filter.NamesQueryFilter;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;

import java.io.DataInput;
import java.io.IOException;

/**
* Created by beggleston on 10/9/14.
*/
public class ThriftCASRequest implements CASRequest
{
    public static final IVersionedSerializer<CASRequest> serializer = new Serializer();

    private final ColumnFamily expected;
    private final ColumnFamily updates;
    private final boolean alwaysApply;

    public ThriftCASRequest(ColumnFamily expected, ColumnFamily updates)
    {
        this(expected, updates, false);
    }

    public ThriftCASRequest(ColumnFamily expected, ColumnFamily updates, boolean alwaysApply)
    {
        this.expected = expected;
        this.updates = updates;
        this.alwaysApply = alwaysApply;
    }

    @Override
    public Type getType()
    {
        return Type.THRIFT;
    }

    public IDiskAtomFilter readFilter()
    {
        return expected == null || expected.isEmpty()
             ? new SliceQueryFilter(ColumnSlice.ALL_COLUMNS_ARRAY, false, 1)
             : new NamesQueryFilter(ImmutableSortedSet.copyOf(expected.getComparator(), expected.getColumnNames()));
    }

    public boolean appliesTo(ColumnFamily current)
    {
        if (alwaysApply)
            return true;

        long now = System.currentTimeMillis();

        if (!hasLiveCells(expected, now))
            return !hasLiveCells(current, now);
        else if (!hasLiveCells(current, now))
            return false;

        // current has been built from expected, so we know that it can't have columns
        // that excepted don't have. So we just check that for each columns in expected:
        //   - if it is a tombstone, whether current has no column or a tombstone;
        //   - otherwise, that current has a live column with the same value.
        for (Cell e : expected)
        {
            Cell c = current.getColumn(e.name());
            if (e.isLive(now))
            {
                if (c == null || !c.isLive(now) || !c.value().equals(e.value()))
                    return false;
            }
            else
            {
                if (c != null && c.isLive(now))
                    return false;
            }
        }
        return true;
    }

    private static boolean hasLiveCells(ColumnFamily cf, long now)
    {
        return cf != null && !cf.hasOnlyTombstones(now);
    }

    public ColumnFamily makeUpdates(ColumnFamily current)
    {
        return updates;
    }

    private static class Serializer implements IVersionedSerializer<CASRequest>
    {
        @Override
        public void serialize(CASRequest request, DataOutputPlus out, int version) throws IOException
        {
            assert request instanceof ThriftCASRequest;
            ThriftCASRequest req = (ThriftCASRequest) request;

            ColumnFamily.serializer.serialize(req.expected, out, version);
            ColumnFamily.serializer.serialize(req.updates, out, version);

            out.writeBoolean(req.alwaysApply);
        }

        @Override
        public CASRequest deserialize(DataInput in, int version) throws IOException
        {
            return new ThriftCASRequest(
                    ColumnFamily.serializer.deserialize(in, version),
                    ColumnFamily.serializer.deserialize(in, version),
                    in.readBoolean()
            );
        }

        @Override
        public long serializedSize(CASRequest request, int version)
        {
            assert request instanceof ThriftCASRequest;
            ThriftCASRequest req = (ThriftCASRequest) request;

            return ColumnFamily.serializer.serializedSize(req.expected, version)
                 + ColumnFamily.serializer.serializedSize(req.updates, version)
                 + 1;
        }
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ThriftCASRequest request = (ThriftCASRequest) o;

        if (expected != null ? !expected.equals(request.expected) : request.expected != null) return false;
        if (updates != null ? !updates.equals(request.updates) : request.updates != null) return false;

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = expected != null ? expected.hashCode() : 0;
        result = 31 * result + (updates != null ? updates.hashCode() : 0);
        return result;
    }

    public ColumnFamily getExpected()
    {
        return expected;
    }

    public ColumnFamily getUpdates()
    {
        return updates;
    }
}
