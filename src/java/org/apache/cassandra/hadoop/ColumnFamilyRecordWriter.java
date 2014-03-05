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
package org.apache.cassandra.hadoop;


import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.thrift.TException;
import org.apache.hadoop.util.Progressable;


/**
 * The <code>ColumnFamilyRecordWriter</code> maps the output &lt;key, value&gt;
 * pairs to a Cassandra column family. In particular, it applies all mutations
 * in the value, which it associates with the key, and in turn the responsible
 * endpoint.
 *
 * <p>
 * Furthermore, this writer groups the mutations by the endpoint responsible for
 * the rows being affected. This allows the mutations to be executed in parallel,
 * directly to a responsible endpoint.
 * </p>
 *
 * @see ColumnFamilyOutputFormat
 */
final class ColumnFamilyRecordWriter extends AbstractColumnFamilyRecordWriter<ByteBuffer, List<Mutation>>
{
    // handles for clients for each range running in the threadpool
    private final Map<Range, RangeClient> clients;
    
    /**
     * Upon construction, obtain the map that this writer will use to collect
     * mutations, and the ring cache for the given keyspace.
     *
     * @param context the task attempt context
     * @throws IOException
     */
    ColumnFamilyRecordWriter(TaskAttemptContext context)
    {
        this(HadoopCompat.getConfiguration(context));
        this.context = context;

    }
    ColumnFamilyRecordWriter(Configuration conf, Progressable progressable)
    {
        this(conf);
        this.progressable = progressable;
    }

    ColumnFamilyRecordWriter(Configuration conf)
    {
        super(conf);
        this.clients = new HashMap<Range, RangeClient>();
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
     * for it with the given column family and columns. In the event the value
     * in the column is missing (i.e., null), then it is marked for
     * {@link Deletion}. Similarly, if the entire value for a key is missing
     * (i.e., null), then the entire key is marked for {@link Deletion}.
     * </p>
     *
     * @param keybuff
     *            the key to write.
     * @param value
     *            the value to write.
     * @throws IOException
     */
    @Override
    public void write(ByteBuffer keybuff, List<Mutation> value) throws IOException
    {
        Range<Token> range = ringCache.getRange(keybuff);

        // get the client for the given range, or create a new one
        RangeClient client = clients.get(range);
        if (client == null)
        {
            // haven't seen keys for this range: create new client
            client = new RangeClient(ringCache.getEndpoint(range));
            client.start();
            clients.put(range, client);
        }

        for (Mutation amut : value)
            client.put(Pair.create(keybuff, amut));
        if (progressable != null)
            progressable.progress();
        if (context != null)
            HadoopCompat.progress(context);
    }

    /**
     * A client that runs in a threadpool and connects to the list of endpoints for a particular
     * range. Mutations for keys in that range are sent to this client via a queue.
     */
    public class RangeClient extends AbstractRangeClient<Pair<ByteBuffer, Mutation>>
    {
        public final String columnFamily = ConfigHelper.getOutputColumnFamily(conf);
        
        /**
        * Constructs an {@link RangeClient} for the given endpoints.
        * @param endpoints the possible endpoints to execute the mutations on
        */
        public RangeClient(List<InetAddress> endpoints)
        {
            super(endpoints);
         }
        
        /**
         * Loops collecting mutations from the queue and sending to Cassandra
         */
        public void run()
        {
            outer:
            while (run || !queue.isEmpty())
            {
                Pair<ByteBuffer, Mutation> mutation;
                try
                {
                    mutation = queue.take();
                }
                catch (InterruptedException e)
                {
                    // re-check loop condition after interrupt
                    continue;
                }

                Map<ByteBuffer, Map<String, List<Mutation>>> batch = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();
                while (mutation != null)
                {
                    Map<String, List<Mutation>> subBatch = batch.get(mutation.left);
                    if (subBatch == null)
                    {
                        subBatch = Collections.singletonMap(columnFamily, (List<Mutation>) new ArrayList<Mutation>());
                        batch.put(mutation.left, subBatch);
                    }

                    subBatch.get(columnFamily).add(mutation.right);
                    if (batch.size() >= batchThreshold)
                        break;

                    mutation = queue.poll();
                }

                Iterator<InetAddress> iter = endpoints.iterator();
                while (true)
                {
                    // send the mutation to the last-used endpoint.  first time through, this will NPE harmlessly.
                    try
                    {
                        client.batch_mutate(batch, consistencyLevel);
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

                    // attempt to connect to a different endpoint
                    try
                    {
                        InetAddress address = iter.next();
                        String host = address.getHostName();
                        int port = ConfigHelper.getOutputRpcPort(conf);
                        client = ColumnFamilyOutputFormat.createAuthenticatedClient(host, port, conf);
                    }
                    catch (Exception e)
                    {
                        closeInternal();
                        // TException means something unexpected went wrong to that endpoint, so
                        // we should try again to another.  Other exceptions (auth or invalid request) are fatal.
                        if ((!(e instanceof TException)) || !iter.hasNext())
                        {
                            lastException = new IOException(e);
                            break outer;
                        }
                    }
                }
            }
        }
    }
}
