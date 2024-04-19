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

package org.apache.cassandra.distributed.impl;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.reads.thresholds.CoordinatorWarnings;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.distributed.impl.Coordinator.toCassandraCL;

public class CoordinatorHelper
{
    public static SimpleQueryResult unsafeExecuteInternal(String query, ConsistencyLevel serialConsistencyLevel, ConsistencyLevel commitConsistencyLevel, Object[] boundValues)
    {
        return unsafeExecuteInternal(query, serialConsistencyLevel, commitConsistencyLevel, Dispatcher.RequestTime.forImmediateExecution(), boundValues);
    }

    public static SimpleQueryResult unsafeExecuteInternal(String query, ConsistencyLevel serialConsistencyLevel, ConsistencyLevel commitConsistencyLevel, Dispatcher.RequestTime requestTime, Object... boundValues)
    {
        ClientState clientState =  makeFakeClientState();
        CQLStatement prepared = QueryProcessor.getStatement(query, clientState);
        List<ByteBuffer> boundBBValues = new ArrayList<>();
        for (Object boundValue : boundValues)
            boundBBValues.add(ByteBufferUtil.objectToBytes(boundValue));

        prepared.validate(QueryState.forInternalCalls().getClientState());
        prepared.validate(clientState);

        // Start capturing warnings on this thread. Note that this will implicitly clear out any previous
        // warnings as it sets a new State instance on the ThreadLocal.
        ClientWarn.instance.captureWarnings();
        CoordinatorWarnings.init();
        try
        {
            ResultMessage res = prepared.execute(QueryState.forInternalCalls(),
                                                 QueryOptions.create(toCassandraCL(commitConsistencyLevel),
                                                                     boundBBValues,
                                                                     false,
                                                                     Integer.MAX_VALUE,
                                                                     null,
                                                                     toCassandraSerialCL(serialConsistencyLevel),
                                                                     ProtocolVersion.CURRENT,
                                                                     null),
                                                 requestTime);
            // Collect warnings reported during the query.
            CoordinatorWarnings.done();
            if (res != null)
                res.setWarnings(ClientWarn.instance.getWarnings());

            return RowUtil.toQueryResult(res);
        }
        catch (Exception | Error e)
        {
            CoordinatorWarnings.done();
            throw e;
        }
        finally
        {
            CoordinatorWarnings.reset();
            ClientWarn.instance.resetWarnings();
        }
    }

    public static ClientState makeFakeClientState()
    {
        return ClientState.forExternalCalls(new InetSocketAddress(FBUtilities.getJustLocalAddress(), 9042));
    }


    protected static org.apache.cassandra.db.ConsistencyLevel toCassandraSerialCL(ConsistencyLevel cl)
    {
        return toCassandraCL(cl == null ? ConsistencyLevel.SERIAL : cl);
    }
}
