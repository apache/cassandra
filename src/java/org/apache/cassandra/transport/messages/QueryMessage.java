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
package org.apache.cassandra.transport.messages;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;

import com.google.common.collect.ImmutableMap;
import org.jboss.netty.buffer.ChannelBuffer;

import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.*;
import org.apache.cassandra.utils.UUIDGen;

/**
 * A CQL query
 */
public class QueryMessage extends Message.Request
{
    public static enum Flag
    {
        // The order of that enum matters!!
        PAGE_SIZE,
        VALUES,
        SKIP_METADATA,
        PAGING_STATE;

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

    public static final Message.Codec<QueryMessage> codec = new Message.Codec<QueryMessage>()
    {
        public QueryMessage decode(ChannelBuffer body, int version)
        {
            String query = CBUtil.readLongString(body);
            ConsistencyLevel consistency = CBUtil.readConsistencyLevel(body);

            int resultPageSize = -1;
            List<ByteBuffer> values = Collections.emptyList();
            boolean skipMetadata = false;
            PagingState pagingState = null;
            if (version >= 2)
            {
                EnumSet<Flag> flags = Flag.deserialize((int)body.readByte());

                if (flags.contains(Flag.PAGE_SIZE))
                    resultPageSize = body.readInt();

                if (flags.contains(Flag.VALUES))
                {
                    int paramCount = body.readUnsignedShort();
                    values = paramCount == 0 ? Collections.<ByteBuffer>emptyList() : new ArrayList<ByteBuffer>(paramCount);
                    for (int i = 0; i < paramCount; i++)
                        values.add(CBUtil.readValue(body));
                }

                skipMetadata = flags.contains(Flag.SKIP_METADATA);

                if (flags.contains(Flag.PAGING_STATE))
                    pagingState = PagingState.deserialize(CBUtil.readValue(body));
            }
            return new QueryMessage(query, consistency, values, resultPageSize, skipMetadata, pagingState);
        }

        public ChannelBuffer encode(QueryMessage msg, int version)
        {
            // We have:
            //   - query
            //   - options
            //     * optional:
            //   - Number of values
            //   - The values
            int vs = msg.values.size();

            EnumSet<Flag> flags = EnumSet.noneOf(Flag.class);
            if (msg.resultPageSize >= 0)
                flags.add(Flag.PAGE_SIZE);
            if (vs > 0)
                flags.add(Flag.VALUES);
            if (msg.skipMetadata)
                flags.add(Flag.SKIP_METADATA);
            if (msg.pagingState != null)
                flags.add(Flag.PAGING_STATE);

            assert flags.isEmpty() || version >= 2 : "Version 1 of the protocol supports no option after the consistency level";

            int nbBuff = 2;
            if (version >= 2)
            {
                nbBuff++; // the flags themselves
                if (flags.contains(Flag.PAGE_SIZE))
                    nbBuff++;
                if (flags.contains(Flag.VALUES))
                    nbBuff++;
            }
            CBUtil.BufferBuilder builder = new CBUtil.BufferBuilder(nbBuff, 0, vs + (flags.contains(Flag.PAGING_STATE) ? 1 : 0));
            builder.add(CBUtil.longStringToCB(msg.query));
            builder.add(CBUtil.consistencyLevelToCB(msg.consistency));
            if (version >= 2)
            {
                builder.add(CBUtil.byteToCB((byte)Flag.serialize(flags)));
                if (flags.contains(Flag.PAGE_SIZE))
                    builder.add(CBUtil.intToCB(msg.resultPageSize));
                if (flags.contains(Flag.VALUES))
                {
                    builder.add(CBUtil.shortToCB(vs));
                    for (ByteBuffer value : msg.values)
                        builder.addValue(value);
                }
                if (flags.contains(Flag.PAGING_STATE))
                    builder.addValue(msg.pagingState == null ? null : msg.pagingState.serialize());
            }
            return builder.build();
        }
    };

    public final String query;
    public final ConsistencyLevel consistency;
    public final int resultPageSize;
    public final List<ByteBuffer> values;
    public final boolean skipMetadata;
    public final PagingState pagingState;

    public QueryMessage(String query, ConsistencyLevel consistency)
    {
        this(query, consistency, Collections.<ByteBuffer>emptyList(), -1);
    }

    public QueryMessage(String query, ConsistencyLevel consistency, List<ByteBuffer> values, int resultPageSize)
    {
        this(query, consistency, values, resultPageSize, false, null);
    }

    public QueryMessage(String query, ConsistencyLevel consistency, List<ByteBuffer> values, int resultPageSize, boolean skipMetadata, PagingState pagingState)
    {
        super(Type.QUERY);
        this.query = query;
        this.consistency = consistency;
        this.resultPageSize = resultPageSize;
        this.values = values;
        this.skipMetadata = skipMetadata;
        this.pagingState = pagingState;
    }

    public ChannelBuffer encode()
    {
        return codec.encode(this, getVersion());
    }

    public Message.Response execute(QueryState state)
    {
        try
        {
            if (resultPageSize == 0)
                throw new ProtocolException("The page size cannot be 0");

            UUID tracingId = null;
            if (isTracingRequested())
            {
                tracingId = UUIDGen.getTimeUUID();
                state.prepareTracingSession(tracingId);
            }

            if (state.traceNextQuery())
            {
                state.createTracingSession();

                ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
                builder.put("query", query);
                if (resultPageSize > 0)
                    builder.put("page_size", Integer.toString(resultPageSize));

                Tracing.instance.begin("Execute CQL3 query", builder.build());
            }

            Message.Response response = QueryProcessor.process(query, values, consistency, state, resultPageSize, pagingState);

            if (tracingId != null)
                response.setTracingId(tracingId);

            return response;
        }
        catch (Exception e)
        {
            if (!((e instanceof RequestValidationException) || (e instanceof RequestExecutionException)))
                logger.error("Unexpected error during query", e);
            return ErrorMessage.fromException(e);
        }
        finally
        {
            Tracing.instance.stopSession();
        }
    }

    @Override
    public String toString()
    {
        return "QUERY " + query;
    }
}
