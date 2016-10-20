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
package org.apache.cassandra.hadoop.cql3;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.*;

import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.HadoopCompat;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.Progressable;

import static java.util.stream.Collectors.toMap;

/**
 * The <code>CqlRecordWriter</code> maps the output &lt;key, value&gt;
 * pairs to a Cassandra table. In particular, it applies the binded variables
 * in the value to the prepared statement, which it associates with the key, and in
 * turn the responsible endpoint.
 *
 * <p>
 * Furthermore, this writer groups the cql queries by the endpoint responsible for
 * the rows being affected. This allows the cql queries to be executed in parallel,
 * directly to a responsible endpoint.
 * </p>
 *
 * @see CqlOutputFormat
 */
class CqlRecordWriter extends RecordWriter<Map<String, ByteBuffer>, List<ByteBuffer>> implements
        org.apache.hadoop.mapred.RecordWriter<Map<String, ByteBuffer>, List<ByteBuffer>>, AutoCloseable
{
    private static final Logger logger = LoggerFactory.getLogger(CqlRecordWriter.class);

    // The configuration this writer is associated with.
    protected final Configuration conf;
    // The number of mutations to buffer per endpoint
    protected final int queueSize;

    protected final long batchThreshold;

    protected Progressable progressable;
    protected TaskAttemptContext context;

    // The ring cache that describes the token ranges each node in the ring is
    // responsible for. This is what allows us to group the mutations by
    // the endpoints they should be targeted at. The targeted endpoint
    // essentially
    // acts as the primary replica for the rows being affected by the mutations.
    private final NativeRingCache ringCache;

    // handles for clients for each range running in the threadpool
    protected final Map<InetAddress, RangeClient> clients;

    // host to prepared statement id mappings
    protected final ConcurrentHashMap<Session, PreparedStatement> preparedStatements = new ConcurrentHashMap<Session, PreparedStatement>();

    protected final String cql;

    protected List<ColumnMetadata> partitionKeyColumns;
    protected List<ColumnMetadata> clusterColumns;

    /**
     * Upon construction, obtain the map that this writer will use to collect
     * mutations, and the ring cache for the given keyspace.
     *
     * @param context the task attempt context
     * @throws IOException
     */
    CqlRecordWriter(TaskAttemptContext context) throws IOException
    {
        this(HadoopCompat.getConfiguration(context));
        this.context = context;
    }

    CqlRecordWriter(Configuration conf, Progressable progressable)
    {
        this(conf);
        this.progressable = progressable;
    }

    CqlRecordWriter(Configuration conf)
    {
        this.conf = conf;
        this.queueSize = conf.getInt(CqlOutputFormat.QUEUE_SIZE, 32 * FBUtilities.getAvailableProcessors());
        batchThreshold = conf.getLong(CqlOutputFormat.BATCH_THRESHOLD, 32);
        this.clients = new HashMap<>();
        String keyspace = ConfigHelper.getOutputKeyspace(conf);

        try (Cluster cluster = CqlConfigHelper.getOutputCluster(ConfigHelper.getOutputInitialAddress(conf), conf))
        {
            Metadata metadata = cluster.getMetadata();
            ringCache = new NativeRingCache(conf, metadata);
            TableMetadata tableMetadata = metadata.getKeyspace(Metadata.quote(keyspace)).getTable(ConfigHelper.getOutputColumnFamily(conf));
            clusterColumns = tableMetadata.getClusteringColumns();
            partitionKeyColumns = tableMetadata.getPartitionKey();

            String cqlQuery = CqlConfigHelper.getOutputCql(conf).trim();
            if (cqlQuery.toLowerCase(Locale.ENGLISH).startsWith("insert"))
                throw new UnsupportedOperationException("INSERT with CqlRecordWriter is not supported, please use UPDATE/DELETE statement");
            cql = appendKeyWhereClauses(cqlQuery);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Close this <code>RecordWriter</code> to future operations, but not before
     * flushing out the batched mutations.
     *
     * @param context the context of the task
     * @throws IOException
     */
    public void close(TaskAttemptContext context) throws IOException, InterruptedException
    {
        close();
    }

    /** Fills the deprecated RecordWriter interface for streaming. */
    @Deprecated
    public void close(org.apache.hadoop.mapred.Reporter reporter) throws IOException
    {
        close();
    }

    @Override
    public void close() throws IOException
    {
        // close all the clients before throwing anything
        IOException clientException = null;
        for (RangeClient client : clients.values())
        {
            try
            {
                client.close();
            }
            catch (IOException e)
            {
                clientException = e;
            }
        }

        if (clientException != null)
            throw clientException;
    }

    /**
     * If the key is to be associated with a valid value, a mutation is created
     * for it with the given table and columns. In the event the value
     * in the column is missing (i.e., null), then it is marked for
     * {@link Deletion}. Similarly, if the entire value for a key is missing
     * (i.e., null), then the entire key is marked for {@link Deletion}.
     * </p>
     *
     * @param keyColumns
     *            the key to write.
     * @param values
     *            the values to write.
     * @throws IOException
     */
    @Override
    public void write(Map<String, ByteBuffer> keyColumns, List<ByteBuffer> values) throws IOException
    {
        TokenRange range = ringCache.getRange(getPartitionKey(keyColumns));

        // get the client for the given range, or create a new one
        final InetAddress address = ringCache.getEndpoints(range).get(0);
        RangeClient client = clients.get(address);
        if (client == null)
        {
            // haven't seen keys for this range: create new client
            client = new RangeClient(ringCache.getEndpoints(range));
            client.start();
            clients.put(address, client);
        }

        // add primary key columns to the bind variables
        List<ByteBuffer> allValues = new ArrayList<ByteBuffer>(values);
        for (ColumnMetadata column : partitionKeyColumns)
            allValues.add(keyColumns.get(column.getName()));
        for (ColumnMetadata column : clusterColumns)
            allValues.add(keyColumns.get(column.getName()));

        client.put(allValues);

        if (progressable != null)
            progressable.progress();
        if (context != null)
            HadoopCompat.progress(context);
    }

    private static void closeSession(Session session)
    {
        //Close the session to satisfy to avoid warnings for the resource not being closed
        try
        {
            if (session != null)
                session.getCluster().closeAsync();
        }
        catch (Throwable t)
        {
            logger.warn("Error closing connection", t);
        }
    }

    /**
     * A client that runs in a threadpool and connects to the list of endpoints for a particular
     * range. Bound variables for keys in that range are sent to this client via a queue.
     */
    public class RangeClient extends Thread
    {
        // The list of endpoints for this range
        protected final List<InetAddress> endpoints;
        protected Cluster cluster = null;
        // A bounded queue of incoming mutations for this range
        protected final BlockingQueue<List<ByteBuffer>> queue = new ArrayBlockingQueue<List<ByteBuffer>>(queueSize);

        protected volatile boolean run = true;
        // we want the caller to know if something went wrong, so we record any unrecoverable exception while writing
        // so we can throw it on the caller's stack when he calls put() again, or if there are no more put calls,
        // when the client is closed.
        protected volatile IOException lastException;

        /**
         * Constructs an {@link RangeClient} for the given endpoints.
         * @param endpoints the possible endpoints to execute the mutations on
         */
        public RangeClient(List<InetAddress> endpoints)
        {
            super("client-" + endpoints);
            this.endpoints = endpoints;
        }

        /**
         * enqueues the given value to Cassandra
         */
        public void put(List<ByteBuffer> value) throws IOException
        {
            while (true)
            {
                if (lastException != null)
                    throw lastException;
                try
                {
                    if (queue.offer(value, 100, TimeUnit.MILLISECONDS))
                        break;
                }
                catch (InterruptedException e)
                {
                    throw new AssertionError(e);
                }
            }
        }

        /**
         * Loops collecting cql binded variable values from the queue and sending to Cassandra
         */
        @SuppressWarnings("resource")
        public void run()
        {
            Session session = null;

            try
            {
                outer:
                while (run || !queue.isEmpty())
                {
                    List<ByteBuffer> bindVariables;
                    try
                    {
                        bindVariables = queue.take();
                    }
                    catch (InterruptedException e)
                    {
                        // re-check loop condition after interrupt
                        continue;
                    }

                    ListIterator<InetAddress> iter = endpoints.listIterator();
                    while (true)
                    {
                        // send the mutation to the last-used endpoint.  first time through, this will NPE harmlessly.
                        if (session != null)
                        {
                            try
                            {
                                int i = 0;
                                PreparedStatement statement = preparedStatement(session);
                                while (bindVariables != null)
                                {
                                    BoundStatement boundStatement = new BoundStatement(statement);
                                    for (int columnPosition = 0; columnPosition < bindVariables.size(); columnPosition++)
                                    {
                                        boundStatement.setBytesUnsafe(columnPosition, bindVariables.get(columnPosition));
                                    }
                                    session.execute(boundStatement);
                                    i++;

                                    if (i >= batchThreshold)
                                        break;
                                    bindVariables = queue.poll();
                                }
                                break;
                            }
                            catch (Exception e)
                            {
                                closeInternal();
                                if (!iter.hasNext())
                                {
                                    lastException = new IOException(e);
                                    break outer;
                                }
                            }
                        }

                        // attempt to connect to a different endpoint
                        try
                        {
                            InetAddress address = iter.next();
                            String host = address.getHostName();
                            cluster = CqlConfigHelper.getOutputCluster(host, conf);
                            closeSession(session);
                            session = cluster.connect();
                        }
                        catch (Exception e)
                        {
                            //If connection died due to Interrupt, just try connecting to the endpoint again.
                            //There are too many ways for the Thread.interrupted() state to be cleared, so
                            //we can't rely on that here. Until the java driver gives us a better way of knowing
                            //that this exception came from an InterruptedException, this is the best solution.
                            if (canRetryDriverConnection(e))
                            {
                                iter.previous();
                            }
                            closeInternal();

                            // Most exceptions mean something unexpected went wrong to that endpoint, so
                            // we should try again to another.  Other exceptions (auth or invalid request) are fatal.
                            if ((e instanceof AuthenticationException || e instanceof InvalidQueryException) || !iter.hasNext())
                            {
                                lastException = new IOException(e);
                                break outer;
                            }
                        }
                    }
                }
            }
            finally
            {
                closeSession(session);
                // close all our connections once we are done.
                closeInternal();
            }
        }

        /** get prepared statement id from cache, otherwise prepare it from Cassandra server*/
        private PreparedStatement preparedStatement(Session client)
        {
            PreparedStatement statement = preparedStatements.get(client);
            if (statement == null)
            {
                PreparedStatement result;
                try
                {
                    result = client.prepare(cql);
                }
                catch (NoHostAvailableException e)
                {
                    throw new RuntimeException("failed to prepare cql query " + cql, e);
                }

                PreparedStatement previousId = preparedStatements.putIfAbsent(client, result);
                statement = previousId == null ? result : previousId;
            }
            return statement;
        }

        public void close() throws IOException
        {
            // stop the run loop.  this will result in closeInternal being called by the time join() finishes.
            run = false;
            interrupt();
            try
            {
                this.join();
            }
            catch (InterruptedException e)
            {
                throw new AssertionError(e);
            }

            if (lastException != null)
                throw lastException;
        }

        protected void closeInternal()
        {
            if (cluster != null)
            {
                cluster.close();
            }
        }

        private boolean canRetryDriverConnection(Exception e)
        {
            if (e instanceof DriverException && e.getMessage().contains("Connection thread interrupted"))
                return true;
            if (e instanceof NoHostAvailableException)
            {
                if (((NoHostAvailableException) e).getErrors().values().size() == 1)
                {
                    Throwable cause = ((NoHostAvailableException) e).getErrors().values().iterator().next();
                    if (cause != null && cause.getCause() instanceof java.nio.channels.ClosedByInterruptException)
                    {
                        return true;
                    }
                }
            }
            return false;
        }
    }

    private ByteBuffer getPartitionKey(Map<String, ByteBuffer> keyColumns)
    {
        ByteBuffer partitionKey;
        if (partitionKeyColumns.size() > 1)
        {
            ByteBuffer[] keys = new ByteBuffer[partitionKeyColumns.size()];
            for (int i = 0; i< keys.length; i++)
                keys[i] = keyColumns.get(partitionKeyColumns.get(i).getName());

            partitionKey = CompositeType.build(keys);
        }
        else
        {
            partitionKey = keyColumns.get(partitionKeyColumns.get(0).getName());
        }
        return partitionKey;
    }

    /**
     * add where clauses for partition keys and cluster columns
     */
    private String appendKeyWhereClauses(String cqlQuery)
    {
        String keyWhereClause = "";

        for (ColumnMetadata partitionKey : partitionKeyColumns)
            keyWhereClause += String.format("%s = ?", keyWhereClause.isEmpty() ? quote(partitionKey.getName()) : (" AND " + quote(partitionKey.getName())));
        for (ColumnMetadata clusterColumn : clusterColumns)
            keyWhereClause += " AND " + quote(clusterColumn.getName()) + " = ?";

        return cqlQuery + " WHERE " + keyWhereClause;
    }

    /** Quoting for working with uppercase */
    private String quote(String identifier)
    {
        return "\"" + identifier.replaceAll("\"", "\"\"") + "\"";
    }

    static class NativeRingCache
    {
        private final Map<TokenRange, Set<Host>> rangeMap;
        private final Metadata metadata;
        private final IPartitioner partitioner;

        public NativeRingCache(Configuration conf, Metadata metadata)
        {
            this.partitioner = ConfigHelper.getOutputPartitioner(conf);
            this.metadata = metadata;
            String keyspace = ConfigHelper.getOutputKeyspace(conf);
            this.rangeMap = metadata.getTokenRanges()
                                    .stream()
                                    .collect(toMap(p -> p, p -> metadata.getReplicas('"' + keyspace + '"', p)));
        }

        public TokenRange getRange(ByteBuffer key)
        {
            Token t = partitioner.getToken(key);
            com.datastax.driver.core.Token driverToken = metadata.newToken(partitioner.getTokenFactory().toString(t));
            for (TokenRange range : rangeMap.keySet())
            {
                if (range.contains(driverToken))
                {
                    return range;
                }
            }

            throw new RuntimeException("Invalid token information returned by describe_ring: " + rangeMap);
        }

        public List<InetAddress> getEndpoints(TokenRange range)
        {
            Set<Host> hostSet = rangeMap.get(range);
            List<InetAddress> addresses = new ArrayList<>(hostSet.size());
            for (Host host: hostSet)
            {
                addresses.add(host.getAddress());
            }
            return addresses;
        }
    }
}
