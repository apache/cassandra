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

package org.apache.cassandra.harry.stress;

import com.datastax.driver.core.*;
import org.apache.cassandra.db.ConsistencyLevel;

import java.util.List;

import com.google.common.util.concurrent.MoreExecutors;

public class ExternalClusterSut
{
    private final Session session;

    public ExternalClusterSut(Session session)
    {
        this(session, 10);
    }

    public ExternalClusterSut(Session session, int threads)
    {
        this.session = session;
    }

    public Session session()
    {
        return session;
    }

    public Metadata metadata()
    {
        return this.session.getCluster().getMetadata();
    }

    public static ExternalClusterSut create(ConsistencyLevel cl, int port, String... contactPoints)
    {
        // TODO: close Cluster and Session!
        return new ExternalClusterSut(Cluster.builder()
                .withQueryOptions(new QueryOptions().setConsistencyLevel(toDriverCl(cl)))
                .addContactPoints(contactPoints)
                .withPort(port)
                .withCredentials("cassandra", "cassandra")
                .build()
                .connect());
    }

    public boolean isShutdown()
    {
        return session.isClosed();
    }

    public void shutdown()
    {
        session.close();
    }

    // TODO: this is rather simplistic
    public Object[][] execute(String statement, ConsistencyLevel cl, Object... bindings)
    {
        return resultSetToObjectArray(session.execute(statement, bindings));
    }

    private static final Object[][] EMPTY = new Object[0][];
    public Object[][] execute(SimpleStatement statement)
    {
        ResultSetFuture future = session.executeAsync(statement);
        return resultSetToObjectArray(future.getUninterruptibly());
    }

    public Object[][] execute(SimpleStatement statement, Runnable callback)
    {
        ResultSetFuture future = session.executeAsync(statement);
        future.addListener(callback, MoreExecutors.directExecutor());
        return resultSetToObjectArray(future.getUninterruptibly());
    }

    public static Object[][] resultSetToObjectArray(ResultSet rs)
    {
        List<Row> rows = rs.all();
        if (rows.size() == 0)
            return new Object[0][];
        Object[][] results = new Object[rows.size()][];
        for (int i = 0; i < results.length; i++)
        {
            Row row = rows.get(i);
            ColumnDefinitions cds = row.getColumnDefinitions();
            Object[] result = new Object[cds.size()];
            for (int j = 0; j < cds.size(); j++)
            {
                if (!row.isNull(j))
                    result[j] = row.getObject(j);
            }
            results[i] = result;
        }
        return results;
    }

    public static com.datastax.driver.core.ConsistencyLevel toDriverCl(ConsistencyLevel cl)
    {
        switch (cl)
        {
            case ONE:
                return com.datastax.driver.core.ConsistencyLevel.ONE;
            case ALL:
                return com.datastax.driver.core.ConsistencyLevel.ALL;
            case QUORUM:
                return com.datastax.driver.core.ConsistencyLevel.QUORUM;
        }
        throw new IllegalArgumentException("Don't know a CL: " + cl);
    }
}