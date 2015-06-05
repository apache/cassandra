package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.CASRequest;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.ByteBufferUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Lists;

public abstract class SerializedRequest <T>
{
    private static final Logger logger = LoggerFactory.getLogger(SerializedRequest.class);

    public static final IVersionedSerializer<SerializedRequest> serializer = new Serializer();

    private static enum Type
    {
        CAS(Serializers.casResult),
        READ(Serializers.readResult);

        private final IVersionedSerializer serializer;
        Type(IVersionedSerializer serializer)
        {
            this.serializer = serializer;
        }
    }

    final String keyspaceName;
    final String cfName;
    final ByteBuffer key;
    final ConsistencyLevel consistencyLevel;
    final Type type;

    private SerializedRequest(Builder builder)
    {
        keyspaceName = builder.keyspaceName;
        cfName = builder.cfName;
        key = builder.key;
        consistencyLevel = builder.consistencyLevel;
        type = builder.type;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SerializedRequest request = (SerializedRequest) o;

        if (!cfName.equals(request.cfName)) return false;
        if (consistencyLevel != request.consistencyLevel) return false;
        if (!key.equals(request.key)) return false;
        if (!keyspaceName.equals(request.keyspaceName)) return false;
        if (type != request.type) return false;

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = keyspaceName.hashCode();
        result = 31 * result + cfName.hashCode();
        result = 31 * result + key.hashCode();
        result = 31 * result + consistencyLevel.hashCode();
        result = 31 * result + type.hashCode();
        return result;
    }

    public String getKeyspaceName()
    {
        return keyspaceName;
    }

    public String getCfName()
    {
        return cfName;
    }

    public ByteBuffer getKey()
    {
        return key;
    }

    public CfKey getCfKey()
    {
        return new CfKey(key, keyspaceName, cfName);
    }

    public ConsistencyLevel getConsistencyLevel()
    {
        return consistencyLevel;
    }

    public IVersionedSerializer getSerializer()
    {
        return type.serializer;
    }

    public abstract ExecutionMetaData<T> execute(long minTimestamp) throws ReadTimeoutException, WriteTimeoutException;

    public Result<T> wrapResult(T value)
    {
        return new Result<>(type, value);
    }

    private static class CAS extends SerializedRequest<ColumnFamily>
    {
        final CASRequest casRequest;

        private CAS(Builder builder)
        {
            super(builder);
            assert builder.type == Type.CAS;
            assert builder.casRequest != null;
            casRequest = builder.casRequest;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;

            CAS cas = (CAS) o;

            if (!casRequest.equals(cas.casRequest)) return false;

            return true;
        }

        @Override
        public int hashCode()
        {
            int result = super.hashCode();
            result = 31 * result + casRequest.hashCode();
            return result;
        }

        /**
         *
         * increases the timestamps in the given column family to be at least
         * the min timestamp given
         */
        static long applyMinTimestamp(ColumnFamily cf, long minTs)
        {
            long delta = minTs - cf.minTimestamp();

            if (delta > 0)
            {
                for (Cell cell: cf)
                {
                    cf.addColumn(cell.withUpdatedTimestamp(cell.timestamp() + delta));
                }

                cf.deletionInfo().applyTimestampDelta(delta);
            }

            return cf.maxTimestamp();
        }


        public ExecutionMetaData<ColumnFamily> execute(long minTimestamp) throws ReadTimeoutException, WriteTimeoutException
        {
            Tracing.trace("Reading existing values for CAS precondition");
            CFMetaData metadata = Schema.instance.getCFMetaData(keyspaceName, cfName);

            // the read timestamp needs to be the same across nodes
            ReadCommand command = ReadCommand.create(keyspaceName, key, cfName, minTimestamp, casRequest.readFilter());

            Keyspace keyspace = Keyspace.open(command.ksName);
            Row row = command.getRow(keyspace);

            ColumnFamily current = row.cf;

            boolean applies;
            try
            {
                applies = casRequest.appliesTo(current);
            }
            catch (InvalidRequestException e)
            {
                throw new RuntimeException(e);
            }

            if (!applies)
            {
                Tracing.trace("CAS precondition does not match current values {}", current);
                logger.debug("CAS precondition does not match current values {}", current);
                // We should not return null as this means success
                ColumnFamily rCF = current == null ? ArrayBackedSortedColumns.factory.create(metadata) : current;
                return new ExecutionMetaData<>(rCF, null, 0, null);
            }
            else
            {
                ReplayPosition rp;
                try
                {
                    ColumnFamily cf = casRequest.makeUpdates(current);
                    long maxTimestamp = applyMinTimestamp(cf, minTimestamp);
                    Mutation mutation = new Mutation(key, cf);
                    rp = Keyspace.open(mutation.getKeyspaceName()).apply(mutation, true);
                    logger.debug("Applying mutation {} at {}", mutation, current);
                    return new ExecutionMetaData<>(null, rp, maxTimestamp, mutation);
                }
                catch (InvalidRequestException e)
                {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private static class Read extends SerializedRequest<List<Row>>
    {
        private final ReadCommand command;

        private Read(Builder builder)
        {
            super(builder);
            assert builder.type == Type.READ;
            assert builder.readCommand != null;
            command = builder.readCommand;
        }

        private static ReadCommand withTs(ReadCommand command, long ts)
        {
            return ReadCommand.create(command.ksName, command.key, command.cfName, ts, command.filter());
        }

        @Override
        public ExecutionMetaData<List<Row>> execute(long minTimestamp) throws ReadTimeoutException, WriteTimeoutException
        {
            Tracing.trace("Executing serialized read");
            logger.debug("Executing serialized read");
            Keyspace keyspace = Keyspace.open(command.ksName);
            Row row = withTs(command, minTimestamp).getRow(keyspace);
            List<Row> result = Lists.newArrayList(row);
            return new ExecutionMetaData<>(result, null, minTimestamp, null);
        }
    }

    public static class ExecutionMetaData<T>
    {
        public final T result;
        public final ReplayPosition replayPosition;
        public final long maxTimestamp;
        public final Mutation mutation;

        public ExecutionMetaData(T result, ReplayPosition replayPosition, long maxTimestamp, Mutation mutation)
        {
            this.result = result;
            this.replayPosition = replayPosition;
            this.maxTimestamp = maxTimestamp;
            this.mutation = mutation;
        }
    }

    public static class Result<T>
    {
        private final Type type;
        private final T value;

        public Result(Type type, T value)
        {
            this.type = type;
            this.value = value;
        }

        public Object getValue()
        {
            return value;
        }

        public static final IVersionedSerializer<Result> serializer = new IVersionedSerializer<Result>()
        {
            @Override
            public void serialize(Result result, DataOutputPlus out, int version) throws IOException
            {
                out.writeByte(result.type.ordinal());
                result.type.serializer.serialize(result.value, out, version);
            }

            @Override
            public Result deserialize(DataInput in, int version) throws IOException
            {
                Type type = Type.values()[in.readByte()];
                return new Result(type, type.serializer.deserialize(in, version));
            }

            @Override
            public long serializedSize(Result result, int version)
            {
                return 1 + result.type.serializer.serializedSize(result.value, version);
            }
        };
    }

    public static class Serializer implements IVersionedSerializer<SerializedRequest>
    {
        @Override
        public void serialize(SerializedRequest request, DataOutputPlus out, int version) throws IOException
        {
            out.writeUTF(request.keyspaceName);
            out.writeUTF(request.cfName);
            ByteBufferUtil.writeWithShortLength(request.key, out);
            out.writeShort(request.consistencyLevel.code);
            out.writeByte(request.type.ordinal());
            switch (request.type)
            {
                case CAS:
                    CASRequest.serializer.serialize(((CAS) request).casRequest, out, version);
                    break;
                case READ:
                    ReadCommand.serializer.serialize(((Read) request).command, out, version);
                    break;
                default:
                    throw new IllegalStateException("Unsupported request type: " + request.type);
            }
        }

        @Override
        public SerializedRequest deserialize(DataInput in, int version) throws IOException
        {
            Builder builder = builder();

            builder.keyspaceName(in.readUTF());
            builder.cfName(in.readUTF());
            builder.key(ByteBufferUtil.readWithShortLength(in));
            builder.consistencyLevel(ConsistencyLevel.fromCode(in.readShort()));
            Type type = Type.values()[in.readByte()];
            switch (type)
            {
                case CAS:
                    builder.casRequest(CASRequest.serializer.deserialize(in, version));
                    break;
                case READ:
                    builder.readCommand(ReadCommand.serializer.deserialize(in, version));
                    break;
                default:
                    throw new IllegalStateException("Unsupported request type: " + type);
            }

            return builder.build();
        }

        @Override
        public long serializedSize(SerializedRequest request, int version)
        {
            long size = 0;
            size += TypeSizes.NATIVE.sizeof(request.keyspaceName);
            size += TypeSizes.NATIVE.sizeof(request.cfName);
            size += TypeSizes.NATIVE.sizeofWithShortLength(request.key);
            size += 2;  // cl
            size += 1;  // type
            switch (request.type)
            {
                case CAS:
                    size += CASRequest.serializer.serializedSize(((CAS) request).casRequest, version);
                    break;
                case READ:
                    size += ReadCommand.serializer.serializedSize(((Read) request).command, version);
                    break;
                default:
                    throw new IllegalStateException("Unsupported request type: " + request.type);
            }

            return size;
        }
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private String keyspaceName;
        private String cfName;
        private ByteBuffer key;
        private ConsistencyLevel consistencyLevel;
        private CASRequest casRequest;
        private ReadCommand readCommand;
        private Type type = null;

        public Builder keyspaceName(String keyspaceName)
        {
            this.keyspaceName = keyspaceName;
            return this;
        }

        public Builder cfName(String cfName)
        {
            this.cfName = cfName;
            return this;
        }

        public Builder key(ByteBuffer key)
        {
            this.key = key;
            return this;
        }

        public Builder consistencyLevel(ConsistencyLevel cl)
        {
            if (!cl.isSerialConsistency())
                throw new IllegalArgumentException("Consistency level must be SERIAL or LOCAL_SERIAL");
            this.consistencyLevel = cl;
            return this;
        }

        private void checkTypeNotSet()
        {
            if (type != null)
            {
                throw new IllegalArgumentException("the type has already been set to " + type);
            }
        }

        public Builder casRequest(CASRequest casRequest)
        {
            checkTypeNotSet();
            this.casRequest = casRequest;
            type = Type.CAS;
            return this;
        }

        public Builder readCommand(ReadCommand readCommand)
        {
            checkTypeNotSet();
            this.readCommand = readCommand;
            type = Type.READ;
            return this;
        }

        public SerializedRequest build()
        {
            switch (type)
            {
                case CAS:
                    return new CAS(this);
                case READ:
                    return new Read(this);
                default:
                    throw new IllegalStateException("unsupported request type: " + type);
            }
        }
    }
}
