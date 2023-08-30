/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.cql3;

import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.collect.ImmutableList;

import io.netty.buffer.ByteBuf;

import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.transport.CBCodec;
import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.ProtocolException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.CassandraUInt;
import org.apache.cassandra.utils.Pair;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * Options for a query.
 */
public abstract class QueryOptions
{
    public static final QueryOptions DEFAULT = new DefaultQueryOptions(ConsistencyLevel.ONE,
                                                                       Collections.emptyList(),
                                                                       false,
                                                                       SpecificOptions.DEFAULT,
                                                                       ProtocolVersion.CURRENT);

    public static final CBCodec<QueryOptions> codec = new Codec();

    private static final long UNSET_NOWINSEC = Long.MIN_VALUE;

    // A cache of bind values parsed as JSON, see getJsonColumnValue for details.
    private List<Map<ColumnIdentifier, Term>> jsonValuesCache;

    public static QueryOptions forInternalCalls(ConsistencyLevel consistency, List<ByteBuffer> values)
    {
        return new DefaultQueryOptions(consistency, values, false, SpecificOptions.DEFAULT, ProtocolVersion.V3);
    }

    public static QueryOptions forInternalCallsWithNowInSec(long nowInSec, ConsistencyLevel consistency, List<ByteBuffer> values)
    {
        return new DefaultQueryOptions(consistency, values, false, SpecificOptions.DEFAULT.withNowInSec(nowInSec), ProtocolVersion.CURRENT);
    }

    public static QueryOptions forInternalCalls(List<ByteBuffer> values)
    {
        return new DefaultQueryOptions(ConsistencyLevel.ONE, values, false, SpecificOptions.DEFAULT, ProtocolVersion.V3);
    }

    public static QueryOptions forProtocolVersion(ProtocolVersion protocolVersion)
    {
        return new DefaultQueryOptions(null, null, true, null, protocolVersion);
    }

    public static QueryOptions create(ConsistencyLevel consistency,
                                      List<ByteBuffer> values,
                                      boolean skipMetadata,
                                      int pageSize,
                                      PagingState pagingState,
                                      ConsistencyLevel serialConsistency,
                                      ProtocolVersion version,
                                      String keyspace)
    {
        return create(consistency, values, skipMetadata, pageSize, pagingState, serialConsistency, version, keyspace, Long.MIN_VALUE, UNSET_NOWINSEC);
    }

    public static QueryOptions create(ConsistencyLevel consistency,
                                      List<ByteBuffer> values,
                                      boolean skipMetadata,
                                      int pageSize,
                                      PagingState pagingState,
                                      ConsistencyLevel serialConsistency,
                                      ProtocolVersion version,
                                      String keyspace,
                                      long timestamp,
                                      long nowInSeconds)
    {
        return new DefaultQueryOptions(consistency,
                                       values,
                                       skipMetadata,
                                       new SpecificOptions(pageSize, pagingState, serialConsistency, timestamp, keyspace, nowInSeconds),
                                       version);
    }

    public static QueryOptions addColumnSpecifications(QueryOptions options, List<ColumnSpecification> columnSpecs)
    {
        return new OptionsWithColumnSpecifications(options, columnSpecs);
    }

    public static QueryOptions withConsistencyLevel(QueryOptions options, ConsistencyLevel consistencyLevel)
    {
        return new OptionsWithConsistencyLevel(options, consistencyLevel);
    }

    public static QueryOptions withPageSize(QueryOptions options, int pageSize)
    {
        return new OptionsWithPageSize(options, pageSize);
    }

    public abstract ConsistencyLevel getConsistency();
    public abstract List<ByteBuffer> getValues();
    public abstract boolean skipMetadata();

    /**
     * Returns the term corresponding to column {@code columnName} in the JSON value of bind index {@code bindIndex}.
     *
     * This is functionally equivalent to:
     *   {@code Json.parseJson(UTF8Type.instance.getSerializer().deserialize(getValues().get(bindIndex)), expectedReceivers).get(columnName)}
     * but this caches the result of parsing the JSON, so that while this might be called for multiple columns on the same {@code bindIndex}
     * value, the underlying JSON value is only parsed/processed once.
     *
     * Note: this is a bit more involved in CQL specifics than this class generally is, but as we need to cache this per-query and in an object
     * that is available when we bind values, this is the easiest place to have this.
     *
     * @param bindIndex the index of the bind value that should be interpreted as a JSON value.
     * @param columnName the name of the column we want the value of.
     * @param expectedReceivers the columns expected in the JSON value at index {@code bindIndex}. This is only used when parsing the
     * json initially and no check is done afterwards. So in practice, any call of this method on the same QueryOptions object and with the same
     * {@code bindIndx} values should use the same value for this parameter, but this isn't validated in any way.
     *
     * @return the value correspong to column {@code columnName} in the (JSON) bind value at index {@code bindIndex}. This may return null if the
     * JSON value has no value for this column.
     */
    public Term getJsonColumnValue(int bindIndex, ColumnIdentifier columnName, Collection<ColumnMetadata> expectedReceivers) throws InvalidRequestException
    {
        if (jsonValuesCache == null)
            jsonValuesCache = new ArrayList<>(Collections.<Map<ColumnIdentifier, Term>>nCopies(getValues().size(), null));

        Map<ColumnIdentifier, Term> jsonValue = jsonValuesCache.get(bindIndex);
        if (jsonValue == null)
        {
            ByteBuffer value = getValues().get(bindIndex);
            if (value == null)
                throw new InvalidRequestException("Got null for INSERT JSON values");

            jsonValue = Json.parseJson(UTF8Type.instance.getSerializer().deserialize(value), expectedReceivers);
            jsonValuesCache.set(bindIndex, jsonValue);
        }

        return jsonValue.get(columnName);
    }

    /**
     * Tells whether or not this <code>QueryOptions</code> contains the column specifications for the bound variables.
     * <p>The column specifications will be present only for prepared statements.</p>
     * @return <code>true</code> this <code>QueryOptions</code> contains the column specifications for the bound
     * variables, <code>false</code> otherwise.
     */
    public boolean hasColumnSpecifications()
    {
        return false;
    }

    /**
     * Returns the column specifications for the bound variables (<i>optional operation</i>).
     *
     * <p>The column specifications will be present only for prepared statements.</p>
     *
     * <p>Invoke the {@link #hasColumnSpecifications} method before invoking this method in order to ensure that this
     * <code>QueryOptions</code> contains the column specifications.</p>
     *
     * @return the option names
     * @throws UnsupportedOperationException If this <code>QueryOptions</code> does not contains the column
     * specifications.
     */
    public ImmutableList<ColumnSpecification> getColumnSpecifications()
    {
        throw new UnsupportedOperationException();
    }

    /**  The pageSize for this query. Will be {@code <= 0} if not relevant for the query.  */
    public int getPageSize()
    {
        return getSpecificOptions().pageSize;
    }

    /** The paging state for this query, or null if not relevant. */
    public PagingState getPagingState()
    {
        return getSpecificOptions().state;
    }

    /**  Serial consistency for conditional updates. */
    public ConsistencyLevel getSerialConsistency()
    {
        return getSpecificOptions().serialConsistency;
    }

    public long getTimestamp(QueryState state)
    {
        long tstamp = getSpecificOptions().timestamp;
        return tstamp != Long.MIN_VALUE ? tstamp : state.getTimestamp();
    }

    public long getNowInSeconds(QueryState state)
    {
        long nowInSeconds = getSpecificOptions().nowInSeconds;
        return nowInSeconds != UNSET_NOWINSEC ? nowInSeconds : state.getNowInSeconds();
    }

    /** The keyspace that this query is bound to, or null if not relevant. */
    public String getKeyspace() { return getSpecificOptions().keyspace; }

    public long getNowInSec(long ifNotSet)
    {
        long nowInSec = getSpecificOptions().nowInSeconds;
        return nowInSec != UNSET_NOWINSEC ? nowInSec : ifNotSet;
    }

    /**
     * The protocol version for the query.
     */
    public abstract ProtocolVersion getProtocolVersion();

    // Mainly for the sake of BatchQueryOptions
    abstract SpecificOptions getSpecificOptions();

    abstract ReadThresholds getReadThresholds();

    public boolean isReadThresholdsEnabled()
    {
        return getReadThresholds().isEnabled();
    }

    public long getCoordinatorReadSizeWarnThresholdBytes()
    {
        return getReadThresholds().getCoordinatorReadSizeWarnThresholdBytes();
    }

    public long getCoordinatorReadSizeAbortThresholdBytes()
    {
        return getReadThresholds().getCoordinatorReadSizeFailThresholdBytes();
    }

    public QueryOptions prepare(List<ColumnSpecification> specs)
    {
        return this;
    }

    interface ReadThresholds
    {
        boolean isEnabled();

        long getCoordinatorReadSizeWarnThresholdBytes();

        long getCoordinatorReadSizeFailThresholdBytes();

        static ReadThresholds create()
        {
            // if daemon initialization hasn't happened yet (very common in tests) then ignore
            if (!DatabaseDescriptor.isDaemonInitialized() || !DatabaseDescriptor.getReadThresholdsEnabled())
                return DisabledReadThresholds.INSTANCE;
            return new DefaultReadThresholds(DatabaseDescriptor.getCoordinatorReadSizeWarnThreshold(), DatabaseDescriptor.getCoordinatorReadSizeFailThreshold());
        }
    }

    private enum DisabledReadThresholds implements ReadThresholds
    {
        INSTANCE;

        @Override
        public boolean isEnabled()
        {
            return false;
        }

        @Override
        public long getCoordinatorReadSizeWarnThresholdBytes()
        {
            return -1;
        }

        @Override
        public long getCoordinatorReadSizeFailThresholdBytes()
        {
            return -1;
        }
    }

    private static class DefaultReadThresholds implements ReadThresholds
    {
        private final long warnThresholdBytes;
        private final long abortThresholdBytes;

        public DefaultReadThresholds(DataStorageSpec.LongBytesBound warnThreshold, DataStorageSpec.LongBytesBound abortThreshold)
        {
            this.warnThresholdBytes = warnThreshold == null ? -1 : warnThreshold.toBytes();
            this.abortThresholdBytes = abortThreshold == null ? -1 : abortThreshold.toBytes();
        }

        @Override
        public boolean isEnabled()
        {
            return true;
        }

        @Override
        public long getCoordinatorReadSizeWarnThresholdBytes()
        {
            return warnThresholdBytes;
        }

        @Override
        public long getCoordinatorReadSizeFailThresholdBytes()
        {
            return abortThresholdBytes;
        }
    }

    static class DefaultQueryOptions extends QueryOptions
    {
        private final ConsistencyLevel consistency;
        private final List<ByteBuffer> values;
        private final boolean skipMetadata;

        private final SpecificOptions options;

        private final transient ProtocolVersion protocolVersion;
        private final transient ReadThresholds readThresholds = ReadThresholds.create();

        DefaultQueryOptions(ConsistencyLevel consistency, List<ByteBuffer> values, boolean skipMetadata, SpecificOptions options, ProtocolVersion protocolVersion)
        {
            this.consistency = consistency;
            this.values = values;
            this.skipMetadata = skipMetadata;
            this.options = options;
            this.protocolVersion = protocolVersion;
        }

        public ConsistencyLevel getConsistency()
        {
            return consistency;
        }

        public List<ByteBuffer> getValues()
        {
            return values;
        }

        public boolean skipMetadata()
        {
            return skipMetadata;
        }

        public ProtocolVersion getProtocolVersion()
        {
            return protocolVersion;
        }

        SpecificOptions getSpecificOptions()
        {
            return options;
        }

        @Override
        ReadThresholds getReadThresholds()
        {
            return readThresholds;
        }
    }

    static class QueryOptionsWrapper extends QueryOptions
    {
        protected final QueryOptions wrapped;

        QueryOptionsWrapper(QueryOptions wrapped)
        {
            this.wrapped = wrapped;
        }

        public List<ByteBuffer> getValues()
        {
            return this.wrapped.getValues();
        }

        public ConsistencyLevel getConsistency()
        {
            return wrapped.getConsistency();
        }

        public boolean skipMetadata()
        {
            return wrapped.skipMetadata();
        }

        public ProtocolVersion getProtocolVersion()
        {
            return wrapped.getProtocolVersion();
        }

        SpecificOptions getSpecificOptions()
        {
            return wrapped.getSpecificOptions();
        }

        @Override
        ReadThresholds getReadThresholds()
        {
            return wrapped.getReadThresholds();
        }

        @Override
        public QueryOptions prepare(List<ColumnSpecification> specs)
        {
            wrapped.prepare(specs);
            return this;
        }
    }

    static class OptionsWithConsistencyLevel extends QueryOptionsWrapper
    {
        private final ConsistencyLevel consistencyLevel;

        OptionsWithConsistencyLevel(QueryOptions wrapped, ConsistencyLevel consistencyLevel)
        {
            super(wrapped);
            this.consistencyLevel = consistencyLevel;
        }

        @Override
        public ConsistencyLevel getConsistency()
        {
            return consistencyLevel;
        }
    }

    static class OptionsWithPageSize extends QueryOptionsWrapper
    {
        private final int pageSize;

        OptionsWithPageSize(QueryOptions wrapped, int pageSize)
        {
            super(wrapped);
            this.pageSize = pageSize;
        }

        @Override
        public int getPageSize()
        {
            return pageSize;
        }
    }

    /**
     * <code>QueryOptions</code> decorator that provides access to the column specifications.
     */
    static class OptionsWithColumnSpecifications extends QueryOptionsWrapper
    {
        private final ImmutableList<ColumnSpecification> columnSpecs;

        OptionsWithColumnSpecifications(QueryOptions wrapped, List<ColumnSpecification> columnSpecs)
        {
            super(wrapped);
            this.columnSpecs = ImmutableList.copyOf(columnSpecs);
        }

        @Override
        public boolean hasColumnSpecifications()
        {
            return true;
        }

        @Override
        public ImmutableList<ColumnSpecification> getColumnSpecifications()
        {
            return columnSpecs;
        }
    }

    static class OptionsWithNames extends QueryOptionsWrapper
    {
        private final List<String> names;
        private List<ByteBuffer> orderedValues;

        OptionsWithNames(DefaultQueryOptions wrapped, List<String> names)
        {
            super(wrapped);
            this.names = names;
        }

        @Override
        public QueryOptions prepare(List<ColumnSpecification> specs)
        {
            super.prepare(specs);

            orderedValues = new ArrayList<>(specs.size());
            for (int i = 0; i < specs.size(); i++)
            {
                String name = specs.get(i).name.toString();
                for (int j = 0; j < names.size(); j++)
                {
                    if (name.equals(names.get(j)))
                    {
                        orderedValues.add(wrapped.getValues().get(j));
                        break;
                    }
                }
            }
            return this;
        }

        @Override
        public List<ByteBuffer> getValues()
        {
            assert orderedValues != null; // We should have called prepare first!
            return orderedValues;
        }
    }

    // Options that are likely to not be present in most queries
    static class SpecificOptions
    {
        private static final SpecificOptions DEFAULT = new SpecificOptions(-1, null, null, Long.MIN_VALUE, null, UNSET_NOWINSEC);

        private final int pageSize;
        private final PagingState state;
        private final ConsistencyLevel serialConsistency;
        private final long timestamp;
        private final String keyspace;
        private final long nowInSeconds;

        private SpecificOptions(int pageSize,
                                PagingState state,
                                ConsistencyLevel serialConsistency,
                                long timestamp,
                                String keyspace,
                                long nowInSeconds)
        {
            this.pageSize = pageSize;
            this.state = state;
            this.serialConsistency = serialConsistency == null ? ConsistencyLevel.SERIAL : serialConsistency;
            this.timestamp = timestamp;
            this.keyspace = keyspace;
            this.nowInSeconds = nowInSeconds;
        }

        public SpecificOptions withNowInSec(long nowInSec)
        {
            return new SpecificOptions(pageSize, state, serialConsistency, timestamp, keyspace, nowInSec);
        }
    }

    private static class Codec implements CBCodec<QueryOptions>
    {
        private enum Flag
        {
            // The order of that enum matters!!
            VALUES,
            SKIP_METADATA,
            PAGE_SIZE,
            PAGING_STATE,
            SERIAL_CONSISTENCY,
            TIMESTAMP,
            NAMES_FOR_VALUES,
            KEYSPACE,
            NOW_IN_SECONDS;

            private static final Flag[] ALL_VALUES = values();

            public static EnumSet<Flag> deserialize(int flags)
            {
                EnumSet<Flag> set = EnumSet.noneOf(Flag.class);
                for (int n = 0; n < ALL_VALUES.length; n++)
                {
                    if ((flags & (1 << n)) != 0)
                        set.add(ALL_VALUES[n]);
                }
                return set;
            }

            public static int serialize(EnumSet<Flag> flags)
            {
                int i = 0;
                for (Flag flag : flags)
                    i |= 1 << flag.ordinal();
                return i;
            }
        }

        public QueryOptions decode(ByteBuf body, ProtocolVersion version)
        {
            ConsistencyLevel consistency = CBUtil.readConsistencyLevel(body);
            EnumSet<Flag> flags = Flag.deserialize(version.isGreaterOrEqualTo(ProtocolVersion.V5)
                                                   ? (int)body.readUnsignedInt()
                                                   : (int)body.readUnsignedByte());

            List<ByteBuffer> values = Collections.<ByteBuffer>emptyList();
            List<String> names = null;
            if (flags.contains(Flag.VALUES))
            {
                if (flags.contains(Flag.NAMES_FOR_VALUES))
                {
                    Pair<List<String>, List<ByteBuffer>> namesAndValues = CBUtil.readNameAndValueList(body, version);
                    names = namesAndValues.left;
                    values = namesAndValues.right;
                }
                else
                {
                    values = CBUtil.readValueList(body, version);
                }
            }

            boolean skipMetadata = flags.contains(Flag.SKIP_METADATA);
            flags.remove(Flag.VALUES);
            flags.remove(Flag.SKIP_METADATA);

            SpecificOptions options = SpecificOptions.DEFAULT;
            if (!flags.isEmpty())
            {
                int pageSize = flags.contains(Flag.PAGE_SIZE) ? body.readInt() : -1;
                PagingState pagingState = flags.contains(Flag.PAGING_STATE) ? PagingState.deserialize(CBUtil.readValueNoCopy(body), version) : null;
                ConsistencyLevel serialConsistency = flags.contains(Flag.SERIAL_CONSISTENCY) ? CBUtil.readConsistencyLevel(body) : ConsistencyLevel.SERIAL;
                long timestamp = Long.MIN_VALUE;
                if (flags.contains(Flag.TIMESTAMP))
                {
                    long ts = body.readLong();
                    if (ts == Long.MIN_VALUE)
                        throw new ProtocolException(String.format("Out of bound timestamp, must be in [%d, %d] (got %d)", Long.MIN_VALUE + 1, Long.MAX_VALUE, ts));
                    timestamp = ts;
                }
                String keyspace = flags.contains(Flag.KEYSPACE) ? CBUtil.readString(body) : null;
                long nowInSeconds = flags.contains(Flag.NOW_IN_SECONDS) ? CassandraUInt.toLong(body.readInt())
                                                                        : UNSET_NOWINSEC;
                options = new SpecificOptions(pageSize, pagingState, serialConsistency, timestamp, keyspace, nowInSeconds);
            }

            DefaultQueryOptions opts = new DefaultQueryOptions(consistency, values, skipMetadata, options, version);
            return names == null ? opts : new OptionsWithNames(opts, names);
        }

        public void encode(QueryOptions options, ByteBuf dest, ProtocolVersion version)
        {
            CBUtil.writeConsistencyLevel(options.getConsistency(), dest);

            EnumSet<Flag> flags = gatherFlags(options, version);
            if (version.isGreaterOrEqualTo(ProtocolVersion.V5))
                dest.writeInt(Flag.serialize(flags));
            else
                dest.writeByte((byte)Flag.serialize(flags));

            if (flags.contains(Flag.VALUES))
                CBUtil.writeValueList(options.getValues(), dest);
            if (flags.contains(Flag.PAGE_SIZE))
                dest.writeInt(options.getPageSize());
            if (flags.contains(Flag.PAGING_STATE))
                CBUtil.writeValue(options.getPagingState().serialize(version), dest);
            if (flags.contains(Flag.SERIAL_CONSISTENCY))
                CBUtil.writeConsistencyLevel(options.getSerialConsistency(), dest);
            if (flags.contains(Flag.TIMESTAMP))
                dest.writeLong(options.getSpecificOptions().timestamp);
            if (flags.contains(Flag.KEYSPACE))
                CBUtil.writeAsciiString(options.getSpecificOptions().keyspace, dest);
            if (flags.contains(Flag.NOW_IN_SECONDS))
                dest.writeInt(CassandraUInt.fromLong(options.getSpecificOptions().nowInSeconds));

            // Note that we don't really have to bother with NAMES_FOR_VALUES server side,
            // and in fact we never really encode QueryOptions, only decode them, so we
            // don't bother.
        }

        public int encodedSize(QueryOptions options, ProtocolVersion version)
        {
            int size = 0;

            size += CBUtil.sizeOfConsistencyLevel(options.getConsistency());

            EnumSet<Flag> flags = gatherFlags(options, version);
            size += (version.isGreaterOrEqualTo(ProtocolVersion.V5) ? 4 : 1);

            if (flags.contains(Flag.VALUES))
                size += CBUtil.sizeOfValueList(options.getValues());
            if (flags.contains(Flag.PAGE_SIZE))
                size += 4;
            if (flags.contains(Flag.PAGING_STATE))
                size += CBUtil.sizeOfValue(options.getPagingState().serializedSize(version));
            if (flags.contains(Flag.SERIAL_CONSISTENCY))
                size += CBUtil.sizeOfConsistencyLevel(options.getSerialConsistency());
            if (flags.contains(Flag.TIMESTAMP))
                size += 8;
            if (flags.contains(Flag.KEYSPACE))
                size += CBUtil.sizeOfAsciiString(options.getSpecificOptions().keyspace);
            if (flags.contains(Flag.NOW_IN_SECONDS))
                size += 4;

            return size;
        }

        private EnumSet<Flag> gatherFlags(QueryOptions options, ProtocolVersion version)
        {
            EnumSet<Flag> flags = EnumSet.noneOf(Flag.class);
            if (options.getValues().size() > 0)
                flags.add(Flag.VALUES);
            if (options.skipMetadata())
                flags.add(Flag.SKIP_METADATA);
            if (options.getPageSize() >= 0)
                flags.add(Flag.PAGE_SIZE);
            if (options.getPagingState() != null)
                flags.add(Flag.PAGING_STATE);
            if (options.getSerialConsistency() != ConsistencyLevel.SERIAL)
                flags.add(Flag.SERIAL_CONSISTENCY);
            if (options.getSpecificOptions().timestamp != Long.MIN_VALUE)
                flags.add(Flag.TIMESTAMP);

            if (version.isGreaterOrEqualTo(ProtocolVersion.V5))
            {
                if (options.getSpecificOptions().keyspace != null)
                    flags.add(Flag.KEYSPACE);
                if (options.getSpecificOptions().nowInSeconds != UNSET_NOWINSEC)
                    flags.add(Flag.NOW_IN_SECONDS);
            }

            return flags;
        }
    }
    
    @Override
    public String toString()
    {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }
}
