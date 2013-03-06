/**
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
package org.apache.cassandra.stress;

import java.io.PrintStream;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;

import com.yammer.metrics.stats.Snapshot;
import org.apache.cassandra.stress.operations.*;
import org.apache.cassandra.stress.util.CassandraClient;
import org.apache.cassandra.stress.util.Operation;
import org.apache.cassandra.transport.SimpleClient;

public class StressAction extends Thread
{
    /**
     * Producer-Consumer model: 1 producer, N consumers
     */
    private final BlockingQueue<Operation> operations = new SynchronousQueue<Operation>(true);

    private final Session client;
    private final PrintStream output;

    private volatile boolean stop = false;

    public static final int SUCCESS = 0;
    public static final int FAILURE = 1;

    private volatile int returnCode = -1;

    public StressAction(Session session, PrintStream out)
    {
        client = session;
        output = out;
    }

    public void run()
    {
        Snapshot latency;
        long oldLatency;
        int epoch, total, oldTotal, keyCount, oldKeyCount;

        // creating keyspace and column families
        if (client.getOperation() == Stress.Operations.INSERT || client.getOperation() == Stress.Operations.COUNTER_ADD)
            client.createKeySpaces();

        int threadCount = client.getThreads();
        Consumer[] consumers = new Consumer[threadCount];

        output.println("total,interval_op_rate,interval_key_rate,latency/95th/99th,elapsed_time");

        int itemsPerThread = client.getKeysPerThread();
        int modulo = client.getNumKeys() % threadCount;

        // creating required type of the threads for the test
        for (int i = 0; i < threadCount; i++) {
            if (i == threadCount - 1)
                itemsPerThread += modulo; // last one is going to handle N + modulo items

            consumers[i] = new Consumer(itemsPerThread);
        }

        Producer producer = new Producer();
        producer.start();

        // starting worker threads
        for (int i = 0; i < threadCount; i++)
            consumers[i].start();

        // initialization of the values
        boolean terminate = false;
        epoch = total = keyCount = 0;

        int interval = client.getProgressInterval();
        int epochIntervals = client.getProgressInterval() * 10;
        long testStartTime = System.currentTimeMillis();

        while (!terminate)
        {
            if (stop)
            {
                producer.stopProducer();

                for (Consumer consumer : consumers)
                    consumer.stopConsume();

                break;
            }

            try
            {
                Thread.sleep(100);
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e.getMessage(), e);
            }

            int alive = 0;
            for (Thread thread : consumers)
                if (thread.isAlive()) alive++;

            if (alive == 0)
                terminate = true;

            epoch++;

            if (terminate || epoch > epochIntervals)
            {
                epoch = 0;

                oldTotal = total;
                oldKeyCount = keyCount;

                total = client.operations.get();
                keyCount = client.keys.get();
                latency = client.latency.getSnapshot();

                int opDelta = total - oldTotal;
                int keyDelta = keyCount - oldKeyCount;

                long currentTimeInSeconds = (System.currentTimeMillis() - testStartTime) / 1000;

                output.println(String.format("%d,%d,%d,%.1f,%.1f,%.1f,%d",
                                             total,
                                             opDelta / interval,
                                             keyDelta / interval,
                                             latency.getMedian(), latency.get95thPercentile(), latency.get999thPercentile(),
                                             currentTimeInSeconds));
            }
        }

        // if any consumer failed, set the return code to failure.
        returnCode = SUCCESS;
        if (producer.isAlive())
        {
            producer.interrupt(); // if producer is still alive it means that we had errors in the consumers
            returnCode = FAILURE;
        }
        for (Consumer consumer : consumers)
            if (consumer.getReturnCode() == FAILURE)
                returnCode = FAILURE;

        if (returnCode == SUCCESS)
            // marking an end of the output to the client
            output.println("END");
        else
            output.println("FAILURE");

    }

    public int getReturnCode()
    {
        return returnCode;
    }

    /**
     * Produces exactly N items (awaits each to be consumed)
     */
    private class Producer extends Thread
    {
        private volatile boolean stop = false;

        public void run()
        {
            for (int i = 0; i < client.getNumKeys(); i++)
            {
                if (stop)
                    break;

                try
                {
                    operations.put(createOperation(i % client.getNumDifferentKeys()));
                }
                catch (InterruptedException e)
                {
                    if (e.getMessage() != null)
                        System.err.println("Producer error - " + e.getMessage());
                    return;
                }
            }
        }

        public void stopProducer()
        {
            stop = true;
        }
    }

    /**
     * Each consumes exactly N items from queue
     */
    private class Consumer extends Thread
    {
        private final int items;
        private volatile boolean stop = false;
        private volatile int returnCode = StressAction.SUCCESS;

        public Consumer(int toConsume)
        {
            items = toConsume;
        }

        public void run()
        {
            if (client.use_native_protocol)
            {
                SimpleClient connection = client.getNativeClient();

                for (int i = 0; i < items; i++)
                {
                    if (stop)
                        break;

                    try
                    {
                        operations.take().run(connection); // running job
                    }
                    catch (Exception e)
                    {
                        if (output == null)
                        {
                            System.err.println(e.getMessage());
                            returnCode = StressAction.FAILURE;
                            System.exit(-1);
                        }

                        output.println(e.getMessage());
                        returnCode = StressAction.FAILURE;
                        break;
                    }
                }
            }
            else
            {
                CassandraClient connection = client.getClient();

                for (int i = 0; i < items; i++)
                {
                    if (stop)
                        break;

                    try
                    {
                        operations.take().run(connection); // running job
                    }
                    catch (Exception e)
                    {
                        if (output == null)
                        {
                            System.err.println(e.getMessage());
                            returnCode = StressAction.FAILURE;
                            System.exit(-1);
                        }

                        output.println(e.getMessage());
                        returnCode = StressAction.FAILURE;
                        break;
                    }
                }
            }
        }

        public void stopConsume()
        {
            stop = true;
        }

        public int getReturnCode()
        {
            return returnCode;
        }
    }

    private Operation createOperation(int index)
    {
        switch (client.getOperation())
        {
            case READ:
                return client.isCQL() ? new CqlReader(client, index) : new Reader(client, index);

            case COUNTER_GET:
                return client.isCQL() ? new CqlCounterGetter(client, index) : new CounterGetter(client, index);

            case INSERT:
                return client.isCQL() ? new CqlInserter(client, index) : new Inserter(client, index);

            case COUNTER_ADD:
                return client.isCQL() ? new CqlCounterAdder(client, index) : new CounterAdder(client, index);

            case RANGE_SLICE:
                return client.isCQL() ? new CqlRangeSlicer(client, index) : new RangeSlicer(client, index);

            case INDEXED_RANGE_SLICE:
                return client.isCQL() ? new CqlIndexedRangeSlicer(client, index) : new IndexedRangeSlicer(client, index);

            case MULTI_GET:
                return client.isCQL() ? new CqlMultiGetter(client, index) : new MultiGetter(client, index);
        }

        throw new UnsupportedOperationException();
    }

    public void stopAction()
    {
        stop = true;
    }
}
