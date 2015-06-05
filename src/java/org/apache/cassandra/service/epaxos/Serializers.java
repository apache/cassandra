package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.UUIDSerializer;

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class Serializers
{
    public static final IVersionedSerializer<Set<UUID>> uuidSets = new IVersionedSerializer<Set<UUID>>()
    {
        @Override
        public void serialize(Set<UUID> uuids, DataOutputPlus out, int version) throws IOException
        {
            out.writeBoolean(uuids != null);
            if (uuids == null)
                return;

            out.writeInt(uuids.size());
            for (UUID id: uuids)
            {
                UUIDSerializer.serializer.serialize(id, out, version);
            }
        }

        @Override
        public Set<UUID> deserialize(DataInput in, int version) throws IOException
        {
            if (!in.readBoolean())
                return null;
            int num = in.readInt();
            Set<UUID> uuids = new HashSet<>(num);
            for (int i=0; i<num; i++)
            {
                uuids.add(UUIDSerializer.serializer.deserialize(in, version));
            }
            return uuids;
        }

        @Override
        public long serializedSize(Set<UUID> uuids, int version)
        {
            if (uuids == null)
                return 1;

            long size = 4 + 1;
            for (UUID id: uuids)
            {
                size += UUIDSerializer.serializer.serializedSize(id, version);
            }
            return size;
        }
    };

    public static final IVersionedSerializer<Range<Token>> tokenRange = new IVersionedSerializer<Range<Token>>()
    {
        @Override
        public void serialize(Range<Token> range, DataOutputPlus out, int version) throws IOException
        {
            Token.serializer.serialize(range.left, out, version);
            Token.serializer.serialize(range.right, out, version);
        }

        @Override
        public Range<Token> deserialize(DataInput in, int version) throws IOException
        {
            return new Range<Token>(Token.serializer.deserialize(in, DatabaseDescriptor.getPartitioner(), version),
                                    Token.serializer.deserialize(in, DatabaseDescriptor.getPartitioner(), version));
        }

        @Override
        public long serializedSize(Range<Token> range, int version)
        {
            return Token.serializer.serializedSize(range.left, version) +
                   Token.serializer.serializedSize(range.left, version);
        }
    };

    public static final IVersionedSerializer<Range<Token>> nullableTokenRange = new IVersionedSerializer<Range<Token>>()
    {
        @Override
        public void serialize(Range<Token> range, DataOutputPlus out, int version) throws IOException
        {
            out.writeBoolean(range != null);
            if (range != null)
            {
                tokenRange.serialize(range, out, version);
            }
        }

        @Override
        public Range<Token> deserialize(DataInput in, int version) throws IOException
        {
            return in.readBoolean() ? tokenRange.deserialize(in, version) : null;
        }

        @Override
        public long serializedSize(Range<Token> range, int version)
        {
            return 1 + (range != null ? tokenRange.serializedSize(range, version) : 0);
        }
    };

    public static final IVersionedSerializer<ColumnFamily> casResult = ColumnFamily.serializer;  // handles null values
    public static final IVersionedSerializer<List<Row>> readResult = new IVersionedSerializer<List<Row>>()
    {
        @Override
        public void serialize(List<Row> rows, DataOutputPlus out, int version) throws IOException
        {
            out.writeInt(rows.size());
            for (Row row: rows)
            {
                Row.serializer.serialize(row, out, version);
            }
        }

        @Override
        public List<Row> deserialize(DataInput in, int version) throws IOException
        {
            int numRows = in.readInt();
            List<Row> rows = new ArrayList<>(numRows);
            for (int i=0; i<numRows; i++)
            {
                rows.add(Row.serializer.deserialize(in, version));
            }
            return rows;
        }

        @Override
        public long serializedSize(List<Row> rows, int version)
        {
            long size = 4;
            for (Row row: rows)
            {
                size += Row.serializer.serializedSize(row, version);
            }
            return size;
        }
    };

    public static final IVersionedSerializer<Map<Scope, ExecutionInfo>> executionMap = new IVersionedSerializer<Map<Scope, ExecutionInfo>>()
    {
        @Override
        public void serialize(Map<Scope, ExecutionInfo> executionInfoMap, DataOutputPlus out, int version) throws IOException
        {
            out.writeInt(executionInfoMap.size());
            for (Map.Entry<Scope, ExecutionInfo> entry: executionInfoMap.entrySet())
            {
                Scope.serializer.serialize(entry.getKey(), out, version);
                ExecutionInfo.serializer.serialize(entry.getValue(), out, version);
            }
        }

        @Override
        public Map<Scope, ExecutionInfo> deserialize(DataInput in, int version) throws IOException
        {
            int num = in.readInt();
            Map<Scope, ExecutionInfo> info = new HashMap<>(num);
            for (int i=0; i<num; i++)
            {
                info.put(Scope.serializer.deserialize(in, version),
                         ExecutionInfo.serializer.deserialize(in, version));
            }

            return info;
        }

        @Override
        public long serializedSize(Map<Scope, ExecutionInfo> executionInfoMap, int version)
        {
            long size = 4;
            for (Map.Entry<Scope, ExecutionInfo> entry: executionInfoMap.entrySet())
            {
                size += Scope.serializer.serializedSize(entry.getKey(), version);
                size += ExecutionInfo.serializer.serializedSize(entry.getValue(), version);
            }
            return size;
        }
    };
}
