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

import org.jboss.netty.buffer.ChannelBuffer;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.transport.CBCodec;
import org.apache.cassandra.transport.CBUtil;

/**
 * Options for a query.
 */
public class QueryOptions
{
    public static final QueryOptions DEFAULT = new QueryOptions(ConsistencyLevel.ONE, Collections.<ByteBuffer>emptyList());

    public static final CBCodec<QueryOptions> codec = new Codec();

    private final ConsistencyLevel consistency;
    private final List<ByteBuffer> values;
    private final boolean skipMetadata;

    private final SpecificOptions options;

    public QueryOptions(ConsistencyLevel consistency, List<ByteBuffer> values)
    {
        this(consistency, values, false, SpecificOptions.DEFAULT);
    }

    public QueryOptions(ConsistencyLevel consistency,
                        List<ByteBuffer> values,
                        boolean skipMetadata,
                        int pageSize,
                        PagingState pagingState,
                        ConsistencyLevel serialConsistency)
    {
        this(consistency, values, skipMetadata, new SpecificOptions(pageSize, pagingState, serialConsistency));
    }

    private QueryOptions(ConsistencyLevel consistency, List<ByteBuffer> values, boolean skipMetadata, SpecificOptions options)
    {
        this.consistency = consistency;
        this.values = values;
        this.skipMetadata = skipMetadata;
        this.options = options;
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

    /**
     * The pageSize for this query. Will be <= 0 if not relevant for the query.
     */
    public int getPageSize()
    {
        return options.pageSize;
    }

    /**
     * The paging state for this query, or null if not relevant.
     */
    public PagingState getPagingState()
    {
        return options.state;
    }

    /**
     * Serial consistency for conditional updates.
     */
    public ConsistencyLevel getSerialConsistency()
    {
        return options.serialConsistency;
    }

    // Options that are likely to not be present in most queries
    private static class SpecificOptions
    {
        private static final SpecificOptions DEFAULT = new SpecificOptions(-1, null, null);

        private final int pageSize;
        private final PagingState state;
        private final ConsistencyLevel serialConsistency;

        private SpecificOptions(int pageSize, PagingState state, ConsistencyLevel serialConsistency)
        {
            this.pageSize = pageSize;
            this.state = state;
            this.serialConsistency = serialConsistency == null ? ConsistencyLevel.SERIAL : serialConsistency;
        }
    }

    private static class Codec implements CBCodec<QueryOptions>
    {
        private static enum Flag
        {
            // The order of that enum matters!!
            VALUES,
            SKIP_METADATA,
            PAGE_SIZE,
            PAGING_STATE,
            SERIAL_CONSISTENCY;

            public static EnumSet<Flag> deserialize(int flags)
            {
                EnumSet<Flag> set = EnumSet.noneOf(Flag.class);
                Flag[] values = Flag.values();
                for (int n = 0; n < values.length; n++)
                {
                    if ((flags & (1 << n)) != 0)
                        set.add(values[n]);
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

        public QueryOptions decode(ChannelBuffer body, int version)
        {
            assert version >= 2;

            ConsistencyLevel consistency = CBUtil.readConsistencyLevel(body);
            EnumSet<Flag> flags = Flag.deserialize((int)body.readByte());

            List<ByteBuffer> values = flags.contains(Flag.VALUES)
                                    ? CBUtil.readValueList(body)
                                    : Collections.<ByteBuffer>emptyList();

            boolean skipMetadata = flags.contains(Flag.SKIP_METADATA);
            flags.remove(Flag.VALUES);
            flags.remove(Flag.SKIP_METADATA);

            SpecificOptions options = SpecificOptions.DEFAULT;
            if (!flags.isEmpty())
            {
                int pageSize = flags.contains(Flag.PAGE_SIZE) ? body.readInt() : -1;
                PagingState pagingState = flags.contains(Flag.PAGING_STATE) ? PagingState.deserialize(CBUtil.readValue(body)) : null;
                ConsistencyLevel serialConsistency = flags.contains(Flag.SERIAL_CONSISTENCY) ? CBUtil.readConsistencyLevel(body) : ConsistencyLevel.SERIAL;
                options = new SpecificOptions(pageSize, pagingState, serialConsistency);
            }
            return new QueryOptions(consistency, values, skipMetadata, options);
        }

        public void encode(QueryOptions options, ChannelBuffer dest, int version)
        {
            assert version >= 2;

            CBUtil.writeConsistencyLevel(options.getConsistency(), dest);

            EnumSet<Flag> flags = gatherFlags(options);
            dest.writeByte((byte)Flag.serialize(flags));

            if (flags.contains(Flag.VALUES))
                CBUtil.writeValueList(options.getValues(), dest);
            if (flags.contains(Flag.PAGE_SIZE))
                dest.writeInt(options.getPageSize());
            if (flags.contains(Flag.PAGING_STATE))
                CBUtil.writeValue(options.getPagingState().serialize(), dest);
            if (flags.contains(Flag.SERIAL_CONSISTENCY))
                CBUtil.writeConsistencyLevel(options.getSerialConsistency(), dest);
        }

        public int encodedSize(QueryOptions options, int version)
        {
            int size = 0;

            size += CBUtil.sizeOfConsistencyLevel(options.getConsistency());

            EnumSet<Flag> flags = gatherFlags(options);
            size += 1;

            if (flags.contains(Flag.VALUES))
                size += CBUtil.sizeOfValueList(options.getValues());
            if (flags.contains(Flag.PAGE_SIZE))
                size += 4;
            if (flags.contains(Flag.PAGING_STATE))
                size += CBUtil.sizeOfValue(options.getPagingState().serialize());
            if (flags.contains(Flag.SERIAL_CONSISTENCY))
                size += CBUtil.sizeOfConsistencyLevel(options.getSerialConsistency());

            return size;
        }

        private EnumSet<Flag> gatherFlags(QueryOptions options)
        {
            EnumSet<Flag> flags = EnumSet.noneOf(Flag.class);
            if (options.getValues().size() > 0)
                flags.add(Flag.VALUES);
            if (options.skipMetadata)
                flags.add(Flag.SKIP_METADATA);
            if (options.getPageSize() >= 0)
                flags.add(Flag.PAGE_SIZE);
            if (options.getPagingState() != null)
                flags.add(Flag.PAGING_STATE);
            if (options.getSerialConsistency() != ConsistencyLevel.SERIAL)
                flags.add(Flag.SERIAL_CONSISTENCY);
            return flags;
        }
    }
}
