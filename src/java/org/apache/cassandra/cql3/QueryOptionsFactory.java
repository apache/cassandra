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
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;

import com.google.common.collect.ImmutableList;

import io.netty.buffer.ByteBuf;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.transport.CBCodec;
import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.ProtocolException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.cql3.AbstractQueryOptions.SpecificOptions;

public final class QueryOptionsFactory
{
    private QueryOptionsFactory() {}

    public static final CBCodec<QueryOptions> codec = new Codec();

    public static final QueryOptions DEFAULT = new DefaultQueryOptions(ConsistencyLevel.ONE,
                                                                       Collections.emptyList(),
                                                                       false,
                                                                       ProtocolVersion.CURRENT);

    public static QueryOptions forInternalCalls(ConsistencyLevel consistency, List<ByteBuffer> values)
    {
        return new DefaultQueryOptions(consistency, values, false, ProtocolVersion.V3);
    }

    public static QueryOptions forInternalCalls(List<ByteBuffer> values)
    {
        return new DefaultQueryOptions(ConsistencyLevel.ONE, values, false, ProtocolVersion.V3);
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
        return create(consistency, values, skipMetadata, pageSize, pagingState, serialConsistency, version, keyspace, Long.MIN_VALUE, Integer.MIN_VALUE, Integer.MAX_VALUE);
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
                                      int nowInSeconds,
                                      int timeoutInMillis)
    {
        return new DefaultQueryOptions(consistency,
                                       values,
                                       skipMetadata,
                                       new SpecificOptions(pageSize, pagingState, serialConsistency, timestamp, keyspace, nowInSeconds, timeoutInMillis),
                                       version);
    }

    public static QueryOptions addColumnSpecifications(QueryOptions options, List<ColumnSpecification> columnSpecs)
    {
        return new OptionsWithColumnSpecifications(options, columnSpecs);
    }

    static class DefaultQueryOptions extends AbstractQueryOptions
    {
        DefaultQueryOptions(ConsistencyLevel consistency, List<ByteBuffer> values, boolean skipMetadata, SpecificOptions options, ProtocolVersion protocolVersion)
        {
            super(consistency, values, skipMetadata, options, protocolVersion);
        }

        DefaultQueryOptions(ConsistencyLevel consistency, List<ByteBuffer> values, boolean skipMetadata, ProtocolVersion protocolVersion)
        {
            super(consistency, values, skipMetadata, protocolVersion);
        }
    }

    /**
     * <code>QueryOptions</code> decorator that provides access to the column specifications.
     */
    static class OptionsWithColumnSpecifications extends AbstractQueryOptions
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

    static class OptionsWithNames extends AbstractQueryOptions
    {
        private final List<String> names;
        private List<ByteBuffer> orderedValues;

        OptionsWithNames(QueryOptions wrapped, List<String> names)
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
                        orderedValues.add(getValues().get(j));
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
            NOW_IN_SECONDS,
            TIMEOUT_IN_MILLIS;

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

            SpecificOptions options = null;
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
                int nowInSeconds = flags.contains(Flag.NOW_IN_SECONDS) ? body.readInt() : Integer.MIN_VALUE;
                int timeoutInMillis = Integer.MAX_VALUE;
                if (flags.contains(Flag.TIMEOUT_IN_MILLIS))
                {
                    int timeout = body.readInt();
                    if (timeout < 1)
                    {
                        throw new ProtocolException(String.format("Out of bound timeout, must be in [1, %d] (got %d)", Integer.MAX_VALUE, timeout));
                    }
                    timeoutInMillis = timeout;
                }
                options = new SpecificOptions(pageSize, pagingState, serialConsistency, timestamp, keyspace, nowInSeconds, timeoutInMillis);
            }

            DefaultQueryOptions opts = options == null
                                       ? new DefaultQueryOptions(consistency, values, skipMetadata, version)
                                       : new DefaultQueryOptions(consistency, values, skipMetadata, options, version);
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
                dest.writeLong(options.getTimestamp());
            if (flags.contains(Flag.KEYSPACE))
                CBUtil.writeString(options.getKeyspace(), dest);
            if (flags.contains(Flag.NOW_IN_SECONDS))
                dest.writeInt(options.getNowInSeconds());
            if (flags.contains(Flag.TIMEOUT_IN_MILLIS))
                dest.writeInt(options.getTimeoutInMillis());

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
                size += CBUtil.sizeOfString(options.getKeyspace());
            if (flags.contains(Flag.NOW_IN_SECONDS))
                size += 4;
            if (flags.contains(Flag.TIMEOUT_IN_MILLIS))
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
            if (options.getTimestamp() != Long.MIN_VALUE)
                flags.add(Flag.TIMESTAMP);

            if (version.isGreaterOrEqualTo(ProtocolVersion.V5))
            {
                if (options.getKeyspace() != null)
                    flags.add(Flag.KEYSPACE);
                if (options.getNowInSeconds() != Integer.MIN_VALUE)
                    flags.add(Flag.NOW_IN_SECONDS);
                if (options.getTimeoutInMillis() != Integer.MIN_VALUE)
                    flags.add(Flag.TIMEOUT_IN_MILLIS);
            }

            return flags;
        }
    }
}
