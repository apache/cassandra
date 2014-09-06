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
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.client.RingCache;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.thrift.transport.TTransport;
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
public abstract class AbstractColumnFamilyRecordWriter<K, Y> extends RecordWriter<K, Y> implements org.apache.hadoop.mapred.RecordWriter<K, Y>
{
    // The configuration this writer is associated with.
    protected final Configuration conf;

    // The ring cache that describes the token ranges each node in the ring is
    // responsible for. This is what allows us to group the mutations by
    // the endpoints they should be targeted at. The targeted endpoint
    // essentially
    // acts as the primary replica for the rows being affected by the mutations.
    protected final RingCache ringCache;

    // The number of mutations to buffer per endpoint
    protected final int queueSize;

    protected final long batchThreshold;

    protected final ConsistencyLevel consistencyLevel;
    protected Progressable progressable;
    protected TaskAttemptContext context;

    protected AbstractColumnFamilyRecordWriter(Configuration conf)
    {
        this.conf = conf;
        this.ringCache = new RingCache(conf);
        this.queueSize = conf.getInt(AbstractColumnFamilyOutputFormat.QUEUE_SIZE, 32 * FBUtilities.getAvailableProcessors());
        batchThreshold = conf.getLong(AbstractColumnFamilyOutputFormat.BATCH_THRESHOLD, 32);
        consistencyLevel = ConsistencyLevel.valueOf(ConfigHelper.getWriteConsistencyLevel(conf));
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
    
    protected abstract void close() throws IOException;

    /**
     * A client that runs in a threadpool and connects to the list of endpoints for a particular
     * range. Mutations for keys in that range are sent to this client via a queue.
     */
    public abstract class AbstractRangeClient<K> extends Thread
    {
        // The list of endpoints for this range
        protected final List<InetAddress> endpoints;
        // A bounded queue of incoming mutations for this range
        protected final BlockingQueue<K> queue = new ArrayBlockingQueue<K>(queueSize);

        protected volatile boolean run = true;
        // we want the caller to know if something went wrong, so we record any unrecoverable exception while writing
        // so we can throw it on the caller's stack when he calls put() again, or if there are no more put calls,
        // when the client is closed.
        protected volatile IOException lastException;

        protected Cassandra.Client client;

        /**
         * Constructs an {@link AbstractRangeClient} for the given endpoints.
         * @param endpoints the possible endpoints to execute the mutations on
         */
        public AbstractRangeClient(List<InetAddress> endpoints)
        {
            super("client-" + endpoints);
            this.endpoints = endpoints;
         }

        /**
         * enqueues the given value to Cassandra
         */
        public void put(K value) throws IOException
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
            if (client != null)
            {
                TTransport transport = client.getOutputProtocol().getTransport();
                if (transport.isOpen())
                    transport.close();
            }
        }

        /**
         * Loops collecting mutations from the queue and sending to Cassandra
         */
        public abstract void run();

        @Override
        public String toString()
        {
            return "#<Client for " + endpoints + ">";
        }
    }
}

