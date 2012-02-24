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
import org.apache.cassandra.utils.UUIDGen;

public class CqlInserter extends Operation
{
    private static List<ByteBuffer> values;

    public CqlInserter(Session client, int idx)
    {
        super(client, idx);
    }

    public void run(Cassandra.Client client) throws IOException
    {
        if (session.getColumnFamilyType() == ColumnFamilyType.Super)
            throw new RuntimeException("Super columns are not implemented for CQL");

        if (values == null)
            values = generateValues();

        StringBuilder query = new StringBuilder("UPDATE Standard1 USING CONSISTENCY ")
                .append(session.getConsistencyLevel().toString()).append(" SET ");

        for (int i = 0; i < session.getColumnsPerKey(); i++)
        {
            if (i > 0)
                query.append(',');

            // Column name
            if (session.timeUUIDComparator)
                query.append(UUIDGen.makeType1UUIDFromHost(Session.getLocalAddress()).toString());
            else
                query.append('C').append(i);

            // Column value
            query.append('=').append(getQuotedCqlBlob(values.get(i % values.size()).array()));
        }

        String key = String.format("%0" + session.getTotalKeysLength() + "d", index);
        query.append(" WHERE KEY=").append(getQuotedCqlBlob(key));

        long start = System.currentTimeMillis();

        boolean success = false;
        String exceptionMessage = null;

        for (int t = 0; t < session.getRetryTimes(); t++)
        {
            if (success)
                break;

            try
            {
                client.execute_cql_query(ByteBuffer.wrap(query.toString().getBytes()), Compression.NONE);
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
