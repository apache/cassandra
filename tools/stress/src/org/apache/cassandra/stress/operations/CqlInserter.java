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
import java.util.ArrayList;
import java.util.List;

import com.yammer.metrics.core.TimerContext;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.stress.Session;
import org.apache.cassandra.stress.util.CassandraClient;
import org.apache.cassandra.stress.util.Operation;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.utils.UUIDGen;

public class CqlInserter extends Operation
{
    private static List<ByteBuffer> values;
    private static String cqlQuery = null;

    public CqlInserter(Session client, int idx)
    {
        super(client, idx);
    }

    public void run(CassandraClient client) throws IOException
    {
        if (session.getColumnFamilyType() == ColumnFamilyType.Super)
            throw new RuntimeException("Super columns are not implemented for CQL");

        if (values == null)
            values = generateValues();

        // Construct a query string once.
        if (cqlQuery == null)
        {
            StringBuilder query = new StringBuilder("UPDATE ").append(wrapInQuotesIfRequired("Standard1"));

            if (session.cqlVersion.startsWith("2"))
                query.append(" USING CONSISTENCY ").append(session.getConsistencyLevel().toString());

            query.append(" SET ");

            for (int i = 0; i < session.getColumnsPerKey(); i++)
            {
                if (i > 0)
                    query.append(',');

                if (session.timeUUIDComparator)
                {
                    if (session.cqlVersion.startsWith("3"))
                        throw new UnsupportedOperationException("Cannot use UUIDs in column names with CQL3");

                    query.append(wrapInQuotesIfRequired(UUIDGen.getTimeUUID().toString()))
                         .append(" = ?");
                }
                else
                {
                    query.append(wrapInQuotesIfRequired("C" + i)).append(" = ?");
                }
            }

            query.append(" WHERE KEY=?");
            cqlQuery = query.toString();
        }

        List<String> queryParms = new ArrayList<String>();
        for (int i = 0; i < session.getColumnsPerKey(); i++)
        {
            // Column value
            queryParms.add(getUnQuotedCqlBlob(values.get(i % values.size()).array(), session.cqlVersion.startsWith("3")));
        }

        String key = String.format("%0" + session.getTotalKeysLength() + "d", index);
        queryParms.add(getUnQuotedCqlBlob(key, session.cqlVersion.startsWith("3")));

        String formattedQuery = null;

        TimerContext context = session.latency.time();

        boolean success = false;
        String exceptionMessage = null;

        for (int t = 0; t < session.getRetryTimes(); t++)
        {
            if (success)
                break;

            try
            {
                if (session.usePreparedStatements())
                {
                    Integer stmntId = getPreparedStatement(client, cqlQuery);
                    if (session.cqlVersion.startsWith("3"))
                        client.execute_prepared_cql3_query(stmntId, queryParamsAsByteBuffer(queryParms), session.getConsistencyLevel());
                    else
                        client.execute_prepared_cql_query(stmntId, queryParamsAsByteBuffer(queryParms));
                }
                else
                {
                    if (formattedQuery == null)
                        formattedQuery = formatCqlQuery(cqlQuery, queryParms);
                    if (session.cqlVersion.startsWith("3"))
                        client.execute_cql3_query(ByteBuffer.wrap(formattedQuery.getBytes()), Compression.NONE, session.getConsistencyLevel());
                    else
                        client.execute_cql_query(ByteBuffer.wrap(formattedQuery.getBytes()), Compression.NONE);
                }

                success = true;
            }
            catch (Exception e)
            {
                exceptionMessage = getExceptionMessage(e);
                success = false;
            }
        }

        if (!success)
        {
            error(String.format("Operation [%d] retried %d times - error inserting key %s %s%n with query %s",
                                index,
                                session.getRetryTimes(),
                                key,
                                (exceptionMessage == null) ? "" : "(" + exceptionMessage + ")",
                                cqlQuery));
        }

        session.operations.getAndIncrement();
        session.keys.getAndIncrement();
        context.stop();
    }
}
