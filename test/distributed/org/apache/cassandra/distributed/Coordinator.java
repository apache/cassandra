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

package org.apache.cassandra.distributed;


import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;


public class Coordinator
{
    final Instance instance;

    public Coordinator(Instance instance)
    {
        this.instance = instance;
    }

    private static Object[][] coordinatorExecute(String query, int consistencyLevel, Object[] bindings)
    {
        CQLStatement prepared = QueryProcessor.getStatement(query, ClientState.forInternalCalls()).statement;
        List<ByteBuffer> boundValues = new ArrayList<>();
        for (Object binding : bindings)
        {
            boundValues.add(ByteBufferUtil.objectToBytes(binding));
        }

        ResultMessage res = prepared.execute(QueryState.forInternalCalls(),
                                             QueryOptions.create(ConsistencyLevel.fromCode(consistencyLevel),
                                                                 boundValues,
                                                                 false,
                                                                 10,
                                                                 null,
                                                                 null,
                                                                 ProtocolVersion.V4),
                                              System.nanoTime());

        if (res != null && res.kind == ResultMessage.Kind.ROWS)
        {
            return RowUtil.toObjects((ResultMessage.Rows) res);
        }
        else
        {
            return new Object[][]{};
        }
    }

    public Object[][] execute(String query, ConsistencyLevel consistencyLevel, Object... boundValues)
    {
        return instance.appliesOnInstance(Coordinator::coordinatorExecute).apply(query, consistencyLevel.code, boundValues);
    }
}
