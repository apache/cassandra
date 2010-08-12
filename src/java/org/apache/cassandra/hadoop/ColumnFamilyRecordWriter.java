package org.apache.cassandra.hadoop;

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
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.cassandra.client.RingCache;
import static org.apache.cassandra.io.SerDeUtils.copy;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Clock;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.Deletion;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.SuperColumn;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.thrift.transport.TSocket;

/**
 * The <code>ColumnFamilyRecordWriter</code> maps the output &lt;key, value&gt;
 * pairs to a Cassandra column family. In particular, it applies all mutations
 * in the value, which it associates with the key, and in turn the responsible
 * endpoint.
 * 
 * <p>
 * Note that, given that round trips to the server are fairly expensive, it
 * merely batches the mutations in-memory and periodically sends the batched
 * mutations to the server in one shot.
 * </p>
 * 
 * <p>
 * Furthermore, this writer groups the mutations by the endpoint responsible for
 * the rows being affected. This allows the mutations to be executed in parallel,
 * directly to a responsible endpoint.
 * </p>
 * 
 * @author Karthick Sankarachary
 * @see ColumnFamilyOutputFormat
 * @see OutputFormat
 * 
 */
final class ColumnFamilyRecordWriter extends RecordWriter<ByteBuffer,List<org.apache.cassandra.avro.Mutation>>
{
    // The task attempt context this writer is associated with.
    private final TaskAttemptContext context;
    
    // The batched set of mutations grouped by endpoints.
    private Map<InetAddress,Map<byte[],Map<String,List<Mutation>>>> mutationsByEndpoint;
    
    // The ring cache that describes the token ranges each node in the ring is
    // responsible for. This is what allows us to group the mutations by
    // the endpoints they should be targeted at. The targeted endpoint
    // essentially
    // acts as the primary replica for the rows being affected by the mutations.
    private final RingCache ringCache;
    
    // The number of mutations currently held in the mutations cache.
    private long batchSize = 0L;
    // The maximum number of mutations to hold in the mutations cache.
    private final long batchThreshold;
    
    /**
     * Upon construction, obtain the map that this writer will use to collect
     * mutations, and the ring cache for the given keyspace.
     * 
     * @param context the task attempt context
     * @throws IOException
     */
    ColumnFamilyRecordWriter(TaskAttemptContext context) throws IOException
    {
        this.context = context;
        this.mutationsByEndpoint = new HashMap<InetAddress,Map<byte[],Map<String,List<Mutation>>>>();
        this.ringCache = new RingCache(ConfigHelper.getOutputKeyspace(context.getConfiguration()),
                                       ConfigHelper.getPartitioner(context.getConfiguration()),
                                       ConfigHelper.getInitialAddress(context.getConfiguration()),
                                       ConfigHelper.getRpcPort(context.getConfiguration()));
        this.batchThreshold = context.getConfiguration().getLong(ColumnFamilyOutputFormat.BATCH_THRESHOLD, Long.MAX_VALUE);
    }
    
    /**
     * Return the endpoint responsible for the given key. The selected endpoint
     * one whose token range contains the given key.
     * 
     * @param key
     *            the key being mutated
     * @return the endpoint responsible for that key
     */
    protected InetAddress getEndpoint(byte[] key)
    {
        return ringCache.getEndpoint(key).iterator().next();
    }

    /**
     * Writes a key/value pair, not to the Cassandra server, but into a
     * in-memory cache (viz. {@link #mutationsByEndpoint}.
     * 
     * <p>
     * If the key is to be associated with a valid value, a mutation is created
     * for it with the given column family and columns. In the event the value
     * in the column is missing (i.e., null), then it is marked for
     * {@link Deletion}. Similarly, if the entire value for a key is missing
     * (i.e., null), then the entire key is marked for {@link Deletion}.
     * </p>
     * 
     * @param key
     *            the key to write.
     * @param value
     *            the value to write.
     * @throws IOException
     */
    @Override
    public synchronized void write(ByteBuffer keybuff, List<org.apache.cassandra.avro.Mutation> value) throws IOException, InterruptedException
    {
        maybeFlush();
        byte[] key = copy(keybuff);
        InetAddress endpoint = getEndpoint(key);
        Map<byte[], Map<String, List<Mutation>>> mutationsByKey = mutationsByEndpoint.get(endpoint);
        if (mutationsByKey == null)
        {
            mutationsByKey = new TreeMap<byte[], Map<String, List<Mutation>>>(FBUtilities.byteArrayComparator);
            mutationsByEndpoint.put(endpoint, mutationsByKey);
        }

        Map<String, List<Mutation>> cfMutation = new HashMap<String, List<Mutation>>();
        mutationsByKey.put(key, cfMutation);

        List<Mutation> mutationList = new ArrayList<Mutation>();
        cfMutation.put(ConfigHelper.getOutputColumnFamily(context.getConfiguration()), mutationList);

        for (org.apache.cassandra.avro.Mutation amut : value)
            mutationList.add(avroToThrift(amut));
    }

    /**
     * Deep copies the given Avro mutation into a new Thrift mutation.
     */
    private Mutation avroToThrift(org.apache.cassandra.avro.Mutation amut)
    {
        Mutation mutation = new Mutation();
        org.apache.cassandra.avro.ColumnOrSuperColumn acosc = amut.column_or_supercolumn;
        if (acosc != null)
        {
            // creation
            ColumnOrSuperColumn cosc = new ColumnOrSuperColumn();
            mutation.setColumn_or_supercolumn(cosc);
            if (acosc.column != null)
                // standard column
                cosc.setColumn(avroToThrift(acosc.column));
            else
            {
                // super column
                byte[] scolname = copy(acosc.super_column.name);
                List<Column> scolcols = new ArrayList<Column>((int)acosc.super_column.columns.size());
                for (org.apache.cassandra.avro.Column acol : acosc.super_column.columns)
                    scolcols.add(avroToThrift(acol));
                cosc.setSuper_column(new SuperColumn(scolname, scolcols));
            }
        }
        else
        {
            // deletion
            Deletion deletion = new Deletion(avroToThrift(amut.deletion.clock));
            mutation.setDeletion(deletion);
            org.apache.cassandra.avro.SlicePredicate apred = amut.deletion.predicate;
            if (amut.deletion.super_column != null)
                // super column
                deletion.setSuper_column(copy(amut.deletion.super_column));
            else if (apred.column_names != null)
            {
                // column names
                List<byte[]> colnames = new ArrayList<byte[]>((int)apred.column_names.size());
                for (ByteBuffer acolname : apred.column_names)
                    colnames.add(copy(acolname));
                deletion.setPredicate(new SlicePredicate().setColumn_names(colnames));
            }
            else
            {
                // range
                deletion.setPredicate(new SlicePredicate().setSlice_range(avroToThrift(apred.slice_range)));
            }
        }
        return mutation;
    }

    private SliceRange avroToThrift(org.apache.cassandra.avro.SliceRange asr)
    {
        return new SliceRange(copy(asr.start), copy(asr.finish), asr.reversed, asr.count);
    }

    private Column avroToThrift(org.apache.cassandra.avro.Column acol)
    {
        return new Column(copy(acol.name), copy(acol.value), avroToThrift(acol.clock));
    }

    private Clock avroToThrift(org.apache.cassandra.avro.Clock aclo)
    {
        return new Clock(aclo.timestamp);
    }

    /**
     * Close this <code>RecordWriter</code> to future operations, but not before
     * flushing out the batched mutations.
     *
     * @param context the context of the task
     * @throws IOException
     */
    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException
    {
        flush();
    }

    /**
     * Flush the mutations cache, iff more mutations have been cached than
     * {@link #batchThreshold}.
     *
     * @throws IOException
     */
    private void maybeFlush() throws IOException
    {
        if (++batchSize > batchThreshold)
        {
            flush();
            batchSize = 0L;
        }
    }

    /**
     * Send the batched mutations over to Cassandra, and then clear the
     * mutations cache.
     *
     * @throws IOException
     */
    protected synchronized void flush() throws IOException
    {
        ExecutorService executor = Executors.newCachedThreadPool();

        try
        {
            List<Future<?>> mutationFutures = new ArrayList<Future<?>>();
            for (Map.Entry<InetAddress, Map<byte[], Map<String, List<Mutation>>>> entry : mutationsByEndpoint.entrySet())
            {
                mutationFutures.add(executor.submit(new EndpointCallable(context, entry.getKey(), entry.getValue())));
            }
            // wait until we have all the results back
            for (Future<?> mutationFuture : mutationFutures)
            {
                try
                {
                    mutationFuture.get();
                }
                catch (ExecutionException e)
                {
                    throw new IOException("Could not perform endpoint mutations", e.getCause());
                }
                catch (InterruptedException e)
                {
                    throw new AssertionError(e);
                }
            }
        }
        finally
        {
            executor.shutdownNow();
            mutationsByEndpoint.clear();
        }

    }

    /**
     * The <code>EndpointCallable</code> facilitates an asynchronous call to a
     * specific node in the ring that commands it to perform a batched set of
     * mutations. Needless to say, the given mutations are targeted at rows that
     * the selected endpoint is responsible for (i.e., is the primary replica
     * for).
     */
    public class EndpointCallable implements Callable<Void>
    {
        // The task attempt context associated with this callable.
        private TaskAttemptContext taskContext;
        // The endpoint of the primary replica for the rows being mutated
        private InetAddress endpoint;
        // The mutations to be performed in the node referenced by {@link
        // #endpoint}.
        private Map<byte[], Map<String, List<Mutation>>> mutations;

        /**
         * Constructs an {@link EndpointCallable} for the given endpoint and set
         * of mutations.
         *
         * @param endpoint  the endpoint wherein to execute the mutations
         * @param mutations the mutation map expected by
         *                  {@link Cassandra.Client#batch_mutate(Map, ConsistencyLevel)}
         */
        public EndpointCallable(TaskAttemptContext taskContext, InetAddress endpoint, Map<byte[], Map<String, List<Mutation>>> mutations)
        {
            this.taskContext = taskContext;
            this.endpoint = endpoint;
            this.mutations = mutations;
        }

        /**
         * Perform the call to
         * {@link Cassandra.Client#batch_mutate(Map, ConsistencyLevel)}.
         */
        public Void call() throws Exception
        {
            TSocket socket = null;
            try
            {
                socket = new TSocket(endpoint.getHostName(), ConfigHelper.getRpcPort(taskContext.getConfiguration()));
                Cassandra.Client client = ColumnFamilyOutputFormat.createAuthenticatedClient(socket, taskContext);
                client.batch_mutate(mutations, ConsistencyLevel.ONE);
                return null;
            }
            finally
            {
                if (socket != null)
                    socket.close();
            }
        }
    }

}
