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

package org.apache.cassandra.tools.fqltool;

import java.io.Closeable;
import java.io.File;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import org.apache.cassandra.utils.FBUtilities;

public class QueryReplayer implements Closeable
{
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
                         String useKeyspace,
                         boolean debug)
    {
        this.queryIterator = queryIterator;
        targetClusters = targetHosts.stream().map(h -> Cluster.builder().addContactPoint(h).build()).collect(Collectors.toList());
        this.filters = filters;
        sessions = useKeyspace != null ?
                   targetClusters.stream().map(c -> c.connect(useKeyspace)).collect(Collectors.toList()) :
                   targetClusters.stream().map(Cluster::connect).collect(Collectors.toList());

        resultHandler = new ResultHandler(targetHosts, resultPaths);
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
                    List<ListenableFuture<ResultSet>> results = new ArrayList<>(sessions.size());
                    for (Session s : sessions)
                    {
                        try
                        {
                            if (query.keyspace != null && !query.keyspace.equals(s.getLoggedKeyspace()))
                            {
                                if (debug)
                                {
                                    out.println(String.format("Switching keyspace from %s to %s", s.getLoggedKeyspace(), query.keyspace));
                                }
                                s.execute("USE " + query.keyspace);
                            }
                        }
                        catch (Throwable t)
                        {
                            out.println("USE "+query.keyspace+" failed: "+t.getMessage());
                        }
                        if (debug)
                        {
                            out.println("Executing query:");
                            out.println(query);
                        }
                        results.add(query.execute(s));
                    }

                    ListenableFuture<List<ResultSet>> resultList = Futures.allAsList(results);

                    Futures.addCallback(resultList, new FutureCallback<List<ResultSet>>()
                    {
                        public void onSuccess(@Nullable List<ResultSet> resultSets)
                        {
                            resultHandler.handleResults(query, resultSets);
                        }

                        public void onFailure(Throwable throwable)
                        {
                            out.println(query + " FAIL: " + throwable);
                        }
                    }, es);

                    FBUtilities.waitOnFuture(resultList);
                }
                catch (Throwable t)
                {
                    out.println("QUERY " + query +" got exception: "+t.getMessage());
                }

                Timer timer = metrics.timer("queries");
                if (timer.getCount() % 5000 == 0)
                    out.printf("%d queries, rate = %.2f%n", timer.getCount(), timer.getOneMinuteRate());
            }
        }
    }

    public void close()
    {
        sessions.forEach(Session::close);
        targetClusters.forEach(Cluster::close);
    }
}
