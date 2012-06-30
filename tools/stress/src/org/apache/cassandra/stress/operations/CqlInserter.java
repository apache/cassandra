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
            StringBuilder query = new StringBuilder("UPDATE Standard1 USING CONSISTENCY ")
                    .append(session.getConsistencyLevel().toString()).append(" SET ");

            for (int i = 0; i < session.getColumnsPerKey(); i++)
            {
                if (i > 0) query.append(',');
                query.append("?=?");
            }

            query.append(" WHERE KEY=?");
            cqlQuery = query.toString();
        }

        List<String> queryParms = new ArrayList<String>();
        for (int i = 0; i < session.getColumnsPerKey(); i++)
        {
            // Column name
            if (session.timeUUIDComparator)
                queryParms.add(UUIDGen.makeType1UUIDFromHost(Session.getLocalAddress()).toString());
            else
                queryParms.add(new String("C" + i));

            // Column value
            queryParms.add(new String(getUnQuotedCqlBlob(values.get(i % values.size()).array())));
        }

        String key = String.format("%0" + session.getTotalKeysLength() + "d", index);
        queryParms.add(new String(getUnQuotedCqlBlob(key)));

        String formattedQuery = null;

        long start = System.currentTimeMillis();

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
                    client.execute_prepared_cql_query(stmntId, queryParamsAsByteBuffer(queryParms));
                }
                else
                {
                    if (formattedQuery == null)
                        formattedQuery = formatCqlQuery(cqlQuery, queryParms);
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
            error(String.format("Operation [%d] retried %d times - error inserting key %s %s%n",
                                index,
                                session.getRetryTimes(),
                                key,
                                (exceptionMessage == null) ? "" : "(" + exceptionMessage + ")"));
        }

        session.operations.getAndIncrement();
        session.keys.getAndIncrement();
        session.latency.getAndAdd(System.currentTimeMillis() - start);
    }
}
