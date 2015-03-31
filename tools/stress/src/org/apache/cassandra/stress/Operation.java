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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.common.util.concurrent.RateLimiter;

import org.apache.cassandra.stress.generate.*;
import org.apache.cassandra.stress.settings.SettingsLog;
import org.apache.cassandra.stress.settings.StressSettings;
import org.apache.cassandra.stress.util.JavaDriverClient;
import org.apache.cassandra.stress.util.ThriftClient;
import org.apache.cassandra.stress.util.Timer;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.transport.SimpleClient;

public abstract class Operation
{
    public final StressSettings settings;
    public final Timer timer;
    protected final DataSpec spec;

    private final List<PartitionIterator> partitionCache = new ArrayList<>();
    protected List<PartitionIterator> partitions;

    public static final class DataSpec
    {
        public final PartitionGenerator partitionGenerator;
        final SeedManager seedManager;
        final Distribution partitionCount;
        final RatioDistribution useRatio;
        final Integer targetCount;

        public DataSpec(PartitionGenerator partitionGenerator, SeedManager seedManager, Distribution partitionCount, Integer targetCount)
        {
            this(partitionGenerator, seedManager, partitionCount, null, targetCount);
        }
        public DataSpec(PartitionGenerator partitionGenerator, SeedManager seedManager, Distribution partitionCount, RatioDistribution useRatio)
        {
            this(partitionGenerator, seedManager, partitionCount, useRatio, null);
        }
        private DataSpec(PartitionGenerator partitionGenerator, SeedManager seedManager, Distribution partitionCount, RatioDistribution useRatio, Integer targetCount)
        {
            this.partitionGenerator = partitionGenerator;
            this.seedManager = seedManager;
            this.partitionCount = partitionCount;
            this.useRatio = useRatio;
            this.targetCount = targetCount;
        }
    }

    public Operation(Timer timer, StressSettings settings, DataSpec spec)
    {
        this.timer = timer;
        this.settings = settings;
        this.spec = spec;
    }

    public static interface RunOp
    {
        public boolean run() throws Exception;
        public int partitionCount();
        public int rowCount();
    }

    boolean ready(WorkManager permits, RateLimiter rateLimiter)
    {
        int partitionCount = (int) spec.partitionCount.next();
        if (partitionCount <= 0)
            return false;
        partitionCount = permits.takePermits(partitionCount);
        if (partitionCount <= 0)
            return false;

        int i = 0;
        boolean success = true;
        for (; i < partitionCount && success ; i++)
        {
            if (i >= partitionCache.size())
                partitionCache.add(PartitionIterator.get(spec.partitionGenerator, spec.seedManager));

            success = false;
            while (!success)
            {
                Seed seed = spec.seedManager.next(this);
                if (seed == null)
                    break;

                if (spec.useRatio == null)
                    success = partitionCache.get(i).reset(seed, spec.targetCount, isWrite());
                else
                    success = partitionCache.get(i).reset(seed, spec.useRatio.next(), isWrite());
            }
        }
        partitionCount = i;

        if (rateLimiter != null)
            rateLimiter.acquire(partitionCount);

        partitions = partitionCache.subList(0, partitionCount);
        return !partitions.isEmpty();
    }

    public boolean isWrite()
    {
        return false;
    }

    /**
     * Run operation
     * @param client Cassandra Thrift client connection
     * @throws IOException on any I/O error.
     */
    public abstract void run(ThriftClient client) throws IOException;

    public void run(SimpleClient client) throws IOException {
        throw new UnsupportedOperationException();
    }

    public void run(JavaDriverClient client) throws IOException {
        throw new UnsupportedOperationException();
    }

    public void timeWithRetry(RunOp run) throws IOException
    {
        timer.start();

        boolean success = false;
        String exceptionMessage = null;

        int tries = 0;
        for (; tries < settings.errors.tries; tries++)
        {
            try
            {
                success = run.run();
                break;
            }
            catch (Exception e)
            {
                switch (settings.log.level)
                {
                    case MINIMAL:
                        break;

                    case NORMAL:
                        System.err.println(e);
                        break;

                    case VERBOSE:
                        e.printStackTrace(System.err);
                        break;

                    default:
                        throw new AssertionError();
                }
                exceptionMessage = getExceptionMessage(e);
            }
        }

        timer.stop(run.partitionCount(), run.rowCount(), !success);

        if (!success)
        {
            error(String.format("Operation x%d on key(s) %s: %s%n",
                    tries,
                    key(),
                    (exceptionMessage == null)
                        ? "Data returned was not validated"
                        : "Error executing: " + exceptionMessage));
        }

    }

    private String key()
    {
        List<String> keys = new ArrayList<>();
        for (PartitionIterator partition : partitions)
            keys.add(partition.getKeyAsString());
        return keys.toString();
    }

    protected String getExceptionMessage(Exception e)
    {
        String className = e.getClass().getSimpleName();
        String message = (e instanceof InvalidRequestException) ? ((InvalidRequestException) e).getWhy() : e.getMessage();
        return (message == null) ? "(" + className + ")" : String.format("(%s): %s", className, message);
    }

    protected void error(String message) throws IOException
    {
        if (!settings.errors.ignore)
            throw new IOException(message);
        else if (settings.log.level.compareTo(SettingsLog.Level.MINIMAL) > 0)
            System.err.println(message);
    }

}
