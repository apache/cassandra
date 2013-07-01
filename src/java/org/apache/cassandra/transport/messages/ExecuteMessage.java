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

import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.PreparedQueryNotFoundException;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.*;
import org.apache.cassandra.utils.MD5Digest;
import org.apache.cassandra.utils.UUIDGen;

public class ExecuteMessage extends Message.Request
{
    public static enum Flag
    {
        // The order of that enum matters!!
        PAGE_SIZE,
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

    public static final Message.Codec<ExecuteMessage> codec = new Message.Codec<ExecuteMessage>()
    {
        public ExecuteMessage decode(ChannelBuffer body, int version)
        {
            byte[] id = CBUtil.readBytes(body);

            int count = body.readUnsignedShort();
            List<ByteBuffer> values = new ArrayList<ByteBuffer>(count);
            for (int i = 0; i < count; i++)
                values.add(CBUtil.readValue(body));

            ConsistencyLevel consistency = CBUtil.readConsistencyLevel(body);

            int resultPageSize = -1;
            boolean skipMetadata = false;
            PagingState pagingState = null;
            if (version >= 2)
            {
                EnumSet<Flag> flags = Flag.deserialize((int)body.readByte());
                if (flags.contains(Flag.PAGE_SIZE))
                    resultPageSize = body.readInt();
                skipMetadata = flags.contains(Flag.SKIP_METADATA);
                if (flags.contains(Flag.PAGING_STATE))
                    pagingState = PagingState.deserialize(CBUtil.readValue(body));
            }
            return new ExecuteMessage(MD5Digest.wrap(id), values, consistency, resultPageSize, skipMetadata, pagingState);
        }

        public ChannelBuffer encode(ExecuteMessage msg, int version)
        {
            // We have:
            //   - statementId
            //   - Number of values
            //   - The values
            //   - options
            int vs = msg.values.size();

            EnumSet<Flag> flags = EnumSet.noneOf(Flag.class);
            if (msg.resultPageSize >= 0)
                flags.add(Flag.PAGE_SIZE);
            if (msg.skipMetadata)
                flags.add(Flag.SKIP_METADATA);
            if (msg.pagingState != null)
                flags.add(Flag.PAGING_STATE);

            assert flags.isEmpty() || version >= 2;

            int nbBuff = 3;
            if (version >= 2)
            {
                nbBuff++; // the flags themselves
                if (flags.contains(Flag.PAGE_SIZE))
                    nbBuff++;
            }
            CBUtil.BufferBuilder builder = new CBUtil.BufferBuilder(nbBuff, 0, vs + (flags.contains(Flag.PAGING_STATE) ? 1 : 0));
            builder.add(CBUtil.bytesToCB(msg.statementId.bytes));
            builder.add(CBUtil.shortToCB(vs));

            // Values
            for (ByteBuffer value : msg.values)
                builder.addValue(value);

            builder.add(CBUtil.consistencyLevelToCB(msg.consistency));

            if (version >= 2)
            {
                builder.add(CBUtil.byteToCB((byte)Flag.serialize(flags)));
                if (flags.contains(Flag.PAGE_SIZE))
                    builder.add(CBUtil.intToCB(msg.resultPageSize));
                if (flags.contains(Flag.PAGING_STATE))
                    builder.addValue(msg.pagingState == null ? null : msg.pagingState.serialize());
            }
            return builder.build();
        }
    };

    public final MD5Digest statementId;
    public final List<ByteBuffer> values;
    public final ConsistencyLevel consistency;
    public final int resultPageSize;
    public final boolean skipMetadata;
    public final PagingState pagingState;

    public ExecuteMessage(byte[] statementId, List<ByteBuffer> values, ConsistencyLevel consistency, int resultPageSize)
    {
        this(MD5Digest.wrap(statementId), values, consistency, resultPageSize, false, null);
    }

    public ExecuteMessage(MD5Digest statementId, List<ByteBuffer> values, ConsistencyLevel consistency, int resultPageSize, boolean skipMetadata, PagingState pagingState)
    {
        super(Message.Type.EXECUTE);
        this.statementId = statementId;
        this.values = values;
        this.consistency = consistency;
        this.resultPageSize = resultPageSize;
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
            CQLStatement statement = QueryProcessor.getPrepared(statementId);

            if (statement == null)
                throw new PreparedQueryNotFoundException(statementId);

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
                if (resultPageSize > 0)
                    builder.put("page_size", Integer.toString(resultPageSize));

                // TODO we don't have [typed] access to CQL bind variables here.  CASSANDRA-4560 is open to add support.
                Tracing.instance.begin("Execute CQL3 prepared query", builder.build());
            }

            Message.Response response = QueryProcessor.processPrepared(statement, consistency, state, values, resultPageSize, pagingState);

            if (tracingId != null)
                response.setTracingId(tracingId);

            return response;
        }
        catch (Exception e)
        {
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
        return "EXECUTE " + statementId + " with " + values.size() + " values at consistency " + consistency;
    }
}
