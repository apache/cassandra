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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;

public class QueryReplayer implements Closeable
{
    private static final Logger logger = LoggerFactory.getLogger(QueryReplayer.class);
    private static final int PRINT_RATE = 5000;
    private final ExecutorService es = executorFactory().sequential("QueryReplayer");
    private final Iterator<List<FQLQuery>> queryIterator;
    private final List<Predicate<FQLQuery>> filters;
    private final List<Session> sessions;
    private final ResultHandler resultHandler;
    private final MetricRegistry metrics = new MetricRegistry();
    private final SessionProvider sessionProvider;

    /**
     * @param queryIterator the queries to be replayed
     * @param targetHosts hosts to connect to, in the format "<user>:<password>@<host>:<port>" where only <host> is mandatory, port defaults to 9042
     * @param resultPaths where to write the results of the queries, for later comparisons, size should be the same as the number of iterators
     * @param filters query filters
     * @param queryFilePathString where to store the queries executed
     */
    public QueryReplayer(Iterator<List<FQLQuery>> queryIterator,
                         List<String> targetHosts,
                         List<File> resultPaths,
                         List<Predicate<FQLQuery>> filters,
                         String queryFilePathString)
    {
        this(queryIterator, targetHosts, resultPaths, filters, queryFilePathString, new DefaultSessionProvider(), null);
    }

    /**
     * Constructor public to allow external users to build their own session provider
     *
     * sessionProvider takes the hosts in targetHosts and creates one session per entry
     */
    public QueryReplayer(Iterator<List<FQLQuery>> queryIterator,
                         List<String> targetHosts,
                         List<File> resultPaths,
                         List<Predicate<FQLQuery>> filters,
                         String queryFilePathString,
                         SessionProvider sessionProvider,
                         MismatchListener mismatchListener)
    {
        this.sessionProvider = sessionProvider;
        this.queryIterator = queryIterator;
        this.filters = filters;
        sessions = targetHosts.stream().map(sessionProvider::connect).collect(Collectors.toList());
        File queryFilePath = queryFilePathString != null ? new File(queryFilePathString) : null;
        resultHandler = new ResultHandler(targetHosts, resultPaths, queryFilePath, mismatchListener);
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
                        if (logger.isDebugEnabled())
                            logger.debug("Executing query: {}", query);
                        ListenableFuture<ResultSet> future = session.executeAsync(statement);
                        results.add(handleErrors(future));
                    }

                    ListenableFuture<List<ResultHandler.ComparableResultSet>> resultList = Futures.allAsList(results);

                    Futures.addCallback(resultList, new FutureCallback<List<ResultHandler.ComparableResultSet>>()
                    {
                        public void onSuccess(List<ResultHandler.ComparableResultSet> resultSets)
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
                    logger.error("QUERY %s got exception: %s", query, t.getMessage());
                }

                Timer timer = metrics.timer("queries");
                if (timer.getCount() % PRINT_RATE == 0)
                    logger.info(String.format("%d queries, rate = %.2f", timer.getCount(), timer.getOneMinuteRate()));
            }
        }
    }

    private void maybeSetKeyspace(Session session, FQLQuery query)
    {
        try
        {
            if (query.keyspace() != null && !query.keyspace().equals(session.getLoggedKeyspace()))
            {
                if (logger.isDebugEnabled())
                    logger.debug("Switching keyspace from {} to {}", session.getLoggedKeyspace(), query.keyspace());
                session.execute("USE " + query.keyspace());
            }
        }
        catch (Throwable t)
        {
            logger.error("USE {} failed: {}", query.keyspace(), t.getMessage());
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
        ListenableFuture<ResultHandler.ComparableResultSet> res = Futures.transform(result, DriverResultSet::new, MoreExecutors.directExecutor());
        return Futures.catching(res, Throwable.class, DriverResultSet::failed, MoreExecutors.directExecutor());
    }

    public void close() throws IOException
    {
        es.shutdown();
        sessionProvider.close();
        resultHandler.close();
    }

    static class ParsedTargetHost
    {
        final int port;
        final String user;
        final String password;
        final String host;

        ParsedTargetHost(String host, int port, String user, String password)
        {
            this.host = host;
            this.port = port;
            this.user = user;
            this.password = password;
        }

        static ParsedTargetHost fromString(String s)
        {
            String [] userInfoHostPort = s.split("@");

            String hostPort = null;
            String user = null;
            String password = null;
            if (userInfoHostPort.length == 2)
            {
                String [] userPassword = userInfoHostPort[0].split(":");
                if (userPassword.length != 2)
                    throw new RuntimeException("Username provided but no password");
                hostPort = userInfoHostPort[1];
                user = userPassword[0];
                password = userPassword[1];
            }
            else if (userInfoHostPort.length == 1)
                hostPort = userInfoHostPort[0];
            else
                throw new RuntimeException("Malformed target host: "+s);

            String[] splitHostPort = hostPort.split(":");
            int port = 9042;
            if (splitHostPort.length == 2)
                port = Integer.parseInt(splitHostPort[1]);

            return new ParsedTargetHost(splitHostPort[0], port, user, password);
        }
    }

    public static interface SessionProvider extends Closeable
    {
        Session connect(String connectionString);
        void close();
    }

    private static final class DefaultSessionProvider implements SessionProvider
    {
        private final static Map<String, Session> sessionCache = new HashMap<>();

        public synchronized Session connect(String connectionString)
        {
            if (sessionCache.containsKey(connectionString))
                return sessionCache.get(connectionString);
            Cluster.Builder builder = Cluster.builder();
            ParsedTargetHost pth = ParsedTargetHost.fromString(connectionString);
            builder.addContactPoint(pth.host);
            builder.withPort(pth.port);
            if (pth.user != null)
                builder.withCredentials(pth.user, pth.password);
            Cluster c = builder.build();
            sessionCache.put(connectionString, c.connect());
            return sessionCache.get(connectionString);
        }

        public void close()
        {
            sessionCache.entrySet().removeIf(entry -> {
                try (Session s = entry.getValue())
                {
                    s.getCluster().close();
                    return true;
                }
                catch (Throwable t)
                {
                    logger.error("Could not close connection", t);
                    return false;
                }
            });
        }
    }
}
