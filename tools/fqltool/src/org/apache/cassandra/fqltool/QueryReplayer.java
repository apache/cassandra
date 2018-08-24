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

package org.apache.cassandra.fqltool;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Predicate;
import java.util.stream.Collectors;

//import javax.annotation.Nullable;

import com.google.common.util.concurrent.FluentFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import org.apache.cassandra.utils.FBUtilities;

public class QueryReplayer implements Closeable
{
    private static final int PRINT_RATE = 5000;
    private final ExecutorService es = Executors.newFixedThreadPool(1);
    private final Iterator<List<FQLQuery>> queryIterator;
    private final List<Cluster> targetClusters;
    private final List<Predicate<FQLQuery>> filters;
    private final List<Session> sessions;
    private final ResultHandler resultHandler;
    private final MetricRegistry metrics = new MetricRegistry();
    private final boolean debug;
    private final PrintStream out;

    public QueryReplayer(Iterator<List<FQLQuery>> queryIterator,
                         List<String> targetHosts,
                         List<File> resultPaths,
                         List<Predicate<FQLQuery>> filters,
                         PrintStream out,
                         String queryFilePathString,
                         boolean debug)
    {
        this.queryIterator = queryIterator;
        targetClusters = targetHosts.stream().map(h -> Cluster.builder().addContactPoint(h).build()).collect(Collectors.toList());
        this.filters = filters;
        sessions = targetClusters.stream().map(Cluster::connect).collect(Collectors.toList());
        File queryFilePath = queryFilePathString != null ? new File(queryFilePathString) : null;
        resultHandler = new ResultHandler(targetHosts, resultPaths, queryFilePath);
        this.debug = debug;
        this.out = out;
    }

    public void replay()
    {
        while (queryIterator.hasNext())
        {
            List<FQLQuery> queries = queryIterator.next();
            for (FQLQuery query : queries)
            {
                if (filters.stream().anyMatch(f -> !f.test(query)))
                    continue;
                try (Timer.Context ctx = metrics.timer("queries").time())
                {
                    List<ListenableFuture<ResultHandler.ComparableResultSet>> results = new ArrayList<>(sessions.size());
                    Statement statement = query.toStatement();
                    for (Session session : sessions)
                    {
                        maybeSetKeyspace(session, query);
                        if (debug)
                        {
                            out.println("Executing query:");
                            out.println(query);
                        }
                        ListenableFuture<ResultSet> future = session.executeAsync(statement);
                        results.add(handleErrors(future));
                    }

                    ListenableFuture<List<ResultHandler.ComparableResultSet>> resultList = Futures.allAsList(results);

                    Futures.addCallback(resultList, new FutureCallback<List<ResultHandler.ComparableResultSet>>()
                    {
                        public void onSuccess(/*@Nullable */List<ResultHandler.ComparableResultSet> resultSets)
                        {
                            // note that the order of resultSets is signifcant here - resultSets.get(x) should
                            // be the result from a query against targetHosts.get(x)
                            resultHandler.handleResults(query, resultSets);
                        }

                        public void onFailure(Throwable throwable)
                        {
                            throw new AssertionError("Errors should be handled in FQLQuery.execute", throwable);
                        }
                    }, es);

                    FBUtilities.waitOnFuture(resultList);
                }
                catch (Throwable t)
                {
                    out.printf("QUERY %s got exception: %s", query, t.getMessage());
                }

                Timer timer = metrics.timer("queries");
                if (timer.getCount() % PRINT_RATE == 0)
                    out.printf("%d queries, rate = %.2f%n", timer.getCount(), timer.getOneMinuteRate());
            }
        }
    }

    private void maybeSetKeyspace(Session session, FQLQuery query)
    {
        try
        {
            if (query.keyspace() != null && !query.keyspace().equals(session.getLoggedKeyspace()))
            {
                if (debug)
                    out.printf("Switching keyspace from %s to %s%n", session.getLoggedKeyspace(), query.keyspace());
                session.execute("USE " + query.keyspace());
            }
        }
        catch (Throwable t)
        {
            out.printf("USE %s failed: %s%n", query.keyspace(), t.getMessage());
        }
    }

    /**
     * Make sure we catch any query errors
     *
     * On error, this creates a failed ComparableResultSet with the exception set to be able to store
     * this fact in the result file and handle comparison of failed result sets.
     */
    private static ListenableFuture<ResultHandler.ComparableResultSet> handleErrors(ListenableFuture<ResultSet> result)
    {
        FluentFuture<ResultHandler.ComparableResultSet> fluentFuture = FluentFuture.from(result)
                                                                                   .transform(DriverResultSet::new, MoreExecutors.directExecutor());
        return fluentFuture.catching(Throwable.class, DriverResultSet::failed, MoreExecutors.directExecutor());
    }

    public void close() throws IOException
    {
        sessions.forEach(Session::close);
        targetClusters.forEach(Cluster::close);
        resultHandler.close();
    }
}
