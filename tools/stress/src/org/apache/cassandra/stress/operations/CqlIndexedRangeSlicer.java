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
import java.util.List;

import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.stress.Session;
import org.apache.cassandra.stress.util.Operation;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlRow;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.utils.Hex.bytesToHex;;

public class CqlIndexedRangeSlicer extends Operation
{
    private static List<ByteBuffer> values = null;
    private static String clauseFragment = "KEY > '%s' LIMIT %d";

    public CqlIndexedRangeSlicer(Session client, int idx)
    {
        super(client, idx);
    }

    public void run(Cassandra.Client client) throws IOException
    {
        if (session.getColumnFamilyType() == ColumnFamilyType.Super)
            throw new RuntimeException("Super columns are not implemented for CQL");

        if (values == null)
            values = generateValues();

        String format = "%0" + session.getTotalKeysLength() + "d";

        String startOffset = String.format(format, 0);
        StringBuilder query = new StringBuilder("SELECT FIRST ").append(session.getColumnsPerKey())
                .append(" ''..'' FROM Standard1 USING CONSISTENCY ").append(session.getConsistencyLevel())
                .append(" WHERE C1 = ").append(getQuotedCqlBlob(values.get(1).array())).append(" AND ");

        int expectedPerValue = session.getNumKeys() / values.size(), received = 0;

        while (received < expectedPerValue)
        {
            long start = System.currentTimeMillis();

            boolean success = false;
            String exceptionMessage = null;
            CqlResult results = null;

            for (int t = 0; t < session.getRetryTimes(); t++)
            {
                if (success)
                    break;

                try
                {
                    ByteBuffer queryBytes = ByteBuffer.wrap(makeQuery(query, startOffset).getBytes());
                    results = client.execute_cql_query(queryBytes, Compression.NONE);
                    success = (results.rows.size() != 0);
                }
                catch (Exception e)
                {
                    exceptionMessage = getExceptionMessage(e);
                    success = false;
                }
            }

            if (!success)
            {
                error(String.format("Operation [%d] retried %d times - error executing indexed range query with offset %s %s%n",
                                    index,
                                    session.getRetryTimes(),
                                    startOffset,
                                    (exceptionMessage == null) ? "" : "(" + exceptionMessage + ")"));
            }

            received += results.rows.size();

            // convert max key found back to an integer, and increment it
            startOffset = String.format(format, (1 + getMaxKey(results.rows)));

            session.operations.getAndIncrement();
            session.keys.getAndAdd(results.rows.size());
            session.latency.getAndAdd(System.currentTimeMillis() - start);
        }
    }

    private String makeQuery(StringBuilder base, String startOffset)
    {
        return base.toString() + String.format(clauseFragment, bytesToHex(startOffset.getBytes()), session.getKeysPerCall());
    }

    /**
     * Get maximum key from CqlRow list
     * @param rows list of the CqlRow objects
     * @return maximum key value of the list
     */
    private int getMaxKey(List<CqlRow> rows)
    {
        int maxKey = ByteBufferUtil.toInt(rows.get(0).key);

        for (CqlRow row : rows)
        {
            int currentKey = ByteBufferUtil.toInt(row.key);
            if (currentKey > maxKey)
                maxKey = currentKey;
        }

        return maxKey;
    }
}
