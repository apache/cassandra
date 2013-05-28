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

import java.util.UUID;

import com.google.common.collect.ImmutableMap;
import org.jboss.netty.buffer.ChannelBuffer;

import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.*;
import org.apache.cassandra.utils.UUIDGen;

public class NextMessage extends Message.Request
{
    public static final Message.Codec<NextMessage> codec = new Message.Codec<NextMessage>()
    {
        public NextMessage decode(ChannelBuffer body, int version)
        {
            int resultPageSize = body.readInt();
            return new NextMessage(resultPageSize);
        }

        public ChannelBuffer encode(NextMessage msg, int version)
        {
            return CBUtil.intToCB(msg.resultPageSize);
        }
    };

    public final int resultPageSize;

    public NextMessage(int resultPageSize)
    {
        super(Type.NEXT);
        this.resultPageSize = resultPageSize;
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

            /*
             * If we had traced the previous page and we are asked to trace this one,
             * record the previous id to allow linking the trace together.
             */
            UUID previousTracingId = state.getAndResetCurrentTracingSession();

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
                if (previousTracingId != null)
                    builder.put("previous_trace", previousTracingId.toString());
                Tracing.instance.begin("Continue paged CQL3 query", builder.build());
            }

            Message.Response response = state.getNextPage(resultPageSize < 0 ? Integer.MAX_VALUE : resultPageSize);

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
            // Trash the current session id if we won't need it anymore
            if (!state.hasPager())
                state.getAndResetCurrentTracingSession();
        }
    }

    @Override
    public String toString()
    {
        return "NEXT " + resultPageSize;
    }
}
