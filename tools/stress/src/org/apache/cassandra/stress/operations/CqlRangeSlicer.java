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
import java.util.Collections;
import java.util.List;

import com.yammer.metrics.core.TimerContext;
import org.apache.cassandra.utils.ByteBufferUtil;

import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.stress.Session;
import org.apache.cassandra.stress.util.CassandraClient;
import org.apache.cassandra.stress.util.Operation;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.transport.SimpleClient;

public class CqlRangeSlicer extends CQLOperation
{
    private static String cqlQuery = null;
    private int lastRowCount;

    public CqlRangeSlicer(Session client, int idx)
    {
        super(client, idx);
    }

    protected void run(CQLQueryExecutor executor) throws IOException
    {
        if (session.getColumnFamilyType() == ColumnFamilyType.Super)
            throw new RuntimeException("Super columns are not implemented for CQL");

        if (cqlQuery == null)
        {
            StringBuilder query = new StringBuilder("SELECT FIRST ").append(session.getColumnsPerKey())
                    .append(" ''..'' FROM Standard1");

            if (session.cqlVersion.startsWith("2"))
                query.append(" USING CONSISTENCY ").append(session.getConsistencyLevel().toString());

            cqlQuery = query.append(" WHERE KEY > ?").toString();
        }

        String key = String.format("%0" +  session.getTotalKeysLength() + "d", index);
        List<String> queryParams = Collections.singletonList(getUnQuotedCqlBlob(key, session.cqlVersion.startsWith("3")));

        TimerContext context = session.latency.time();

        boolean success = false;
        String exceptionMessage = null;

        for (int t = 0; t < session.getRetryTimes(); t++)
        {
            if (success)
                break;

            try
            {
                success = executor.execute(cqlQuery, queryParams);
            }
            catch (Exception e)
            {
                System.err.println(e);
                exceptionMessage = getExceptionMessage(e);
                success = false;
            }
        }

        if (!success)
        {
            error(String.format("Operation [%d] retried %d times - error executing range slice with offset %s %s%n",
                                index,
                                session.getRetryTimes(),
                                key,
                                (exceptionMessage == null) ? "" : "(" + exceptionMessage + ")"));
        }

        session.operations.getAndIncrement();
        session.keys.getAndAdd(lastRowCount);
        context.stop();
    }

    protected boolean validateThriftResult(CqlResult result)
    {
        lastRowCount = result.rows.size();
        return  lastRowCount != 0;
    }

    protected boolean validateNativeResult(ResultMessage result)
    {
        assert result instanceof ResultMessage.Rows;
        lastRowCount = ((ResultMessage.Rows)result).result.size();
        return lastRowCount != 0;
    }
}
