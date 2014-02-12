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


import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.apache.cassandra.stress.settings.SettingsCommandMulti;
import org.apache.cassandra.utils.FBUtilities;

public class CqlIndexedRangeSlicer extends CqlOperation<byte[][]>
{

    volatile boolean acceptNoResults = false;

    public CqlIndexedRangeSlicer(State state, long idx)
    {
        super(state, idx);
    }

    @Override
    protected List<ByteBuffer> getQueryParameters(byte[] key)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected String buildQuery()
    {
        StringBuilder query = new StringBuilder("SELECT ");

        if (state.isCql2())
            query.append(state.settings.columns.maxColumnsPerKey).append(" ''..''");
        else
            query.append("*");

        query.append(" FROM Standard1");

        if (state.isCql2())
            query.append(" USING CONSISTENCY ").append(state.settings.command.consistencyLevel);

        final String columnName = getColumnName(1);
        query.append(" WHERE ").append(columnName).append("=?")
                .append(" AND KEY > ? LIMIT ").append(((SettingsCommandMulti)state.settings.command).keysAtOnce);
        return query.toString();
    }

    @Override
    protected void run(CqlOperation.ClientWrapper client) throws IOException
    {
        acceptNoResults = false;
        final List<ByteBuffer> columns = generateColumnValues(getKey());
        final ByteBuffer value = columns.get(1); // only C1 column is indexed
        byte[] minKey = new byte[0];
        int rowCount;
        do
        {
            List<ByteBuffer> params = Arrays.asList(value, ByteBuffer.wrap(minKey));
            CqlRunOp<byte[][]> op = run(client, params, value, new String(value.array()));
            byte[][] keys = op.result;
            rowCount = keys.length;
            minKey = getNextMinKey(minKey, keys);
            acceptNoResults = true;
        } while (rowCount > 0);
    }

    private final class IndexedRangeSliceRunOp extends CqlRunOpFetchKeys
    {

        protected IndexedRangeSliceRunOp(ClientWrapper client, String query, Object queryId, List<ByteBuffer> params, String keyid, ByteBuffer key)
        {
            super(client, query, queryId, params, keyid, key);
        }

        @Override
        public boolean validate(byte[][] result)
        {
            return acceptNoResults || result.length > 0;
        }
    }

    @Override
    protected CqlRunOp<byte[][]> buildRunOp(ClientWrapper client, String query, Object queryId, List<ByteBuffer> params, String keyid, ByteBuffer key)
    {
        return new IndexedRangeSliceRunOp(client, query, queryId, params, keyid, key);
    }

    private static byte[] getNextMinKey(byte[] cur, byte[][] keys)
    {
        // find max
        for (byte[] key : keys)
            if (FBUtilities.compareUnsigned(cur, key) < 0)
                cur = key;

        // increment
        for (int i = 0 ; i < cur.length ; i++)
            if (++cur[i] != 0)
                break;
        return cur;
    }

}
