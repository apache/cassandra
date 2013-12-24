package org.apache.cassandra.stress.operations;
/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */


import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

public class CqlRangeSlicer extends CqlOperation<Integer>
{
    public CqlRangeSlicer(State state, long idx)
    {
        super(state, idx);
    }

    @Override
    protected List<ByteBuffer> getQueryParameters(byte[] key)
    {
        return Collections.singletonList(ByteBuffer.wrap(key));
    }

    @Override
    protected String buildQuery()
    {
        StringBuilder query = new StringBuilder("SELECT FIRST ").append(state.settings.columns.maxColumnsPerKey)
                .append(" ''..'' FROM ").append(state.settings.schema.columnFamily);

        if (state.isCql2())
            query.append(" USING CONSISTENCY ").append(state.settings.command.consistencyLevel);

        return query.append(" WHERE KEY > ?").toString();
    }

    @Override
    protected CqlRunOp buildRunOp(ClientWrapper client, String query, Object queryId, List<ByteBuffer> params, String keyid, ByteBuffer key)
    {
        return new CqlRunOpTestNonEmpty(client, query, queryId, params, keyid, key);
    }

}
