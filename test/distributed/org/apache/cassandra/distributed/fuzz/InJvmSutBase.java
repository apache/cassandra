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

package org.apache.cassandra.distributed.fuzz;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import com.google.common.collect.Iterators;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import harry.core.Configuration;
import harry.model.sut.SystemUnderTest;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IMessage;
import org.apache.cassandra.distributed.api.IMessageFilters;

public class InJvmSutBase<NODE extends IInstance, CLUSTER extends ICluster<NODE>> implements SystemUnderTest.FaultInjectingSut
{
    public static void init()
    {
        Configuration.registerSubtypes(InJvmSutBaseConfiguration.class);
    }

    private static final Logger logger = LoggerFactory.getLogger(InJvmSutBase.class);

    private final ExecutorService executor;
    public final CLUSTER cluster;
    private final AtomicLong cnt = new AtomicLong();
    private final AtomicBoolean isShutdown = new AtomicBoolean(false);

    public InJvmSutBase(CLUSTER cluster)
    {
        this(cluster, 10);
    }

    public InJvmSutBase(CLUSTER cluster, int threads)
    {
        this.cluster = cluster;
        this.executor = Executors.newFixedThreadPool(threads);
    }

    public CLUSTER cluster()
    {
        return cluster;
    }

    @Override
    public boolean isShutdown()
    {
        return isShutdown.get();
    }

    @Override
    public void shutdown()
    {
        assert isShutdown.compareAndSet(false, true);

        try
        {
            cluster.close();
            executor.shutdown();
            if (!executor.awaitTermination(30, TimeUnit.SECONDS))
                throw new TimeoutException("Could not terminate cluster within expected timeout");
        }
        catch (Throwable e)
        {
            logger.error("Could not terminate cluster.", e);
            throw new RuntimeException(e);
        }
    }

    public void schemaChange(String statement)
    {
        cluster.schemaChange(statement);
    }

    public Object[][] execute(String statement, ConsistencyLevel cl, Object... bindings)
    {
        return execute(statement, cl, (int) (cnt.getAndIncrement() % cluster.size() + 1), bindings);
    }

    public Object[][] execute(String statement, ConsistencyLevel cl, int coordinator, Object... bindings)
    {
        if (isShutdown.get())
            throw new RuntimeException("Instance is shut down");

        try
        {
            if (cl == ConsistencyLevel.NODE_LOCAL)
            {
                return cluster.get(coordinator)
                              .executeInternal(statement, bindings);
            }
            else if (StringUtils.startsWithIgnoreCase(statement, "SELECT"))
            {
                return Iterators.toArray(cluster
                                         // round-robin
                                         .coordinator(coordinator)
                                         .executeWithPaging(statement, toApiCl(cl), 1, bindings),
                                         Object[].class);
            }
            else
            {
                return cluster
                       // round-robin
                       .coordinator(coordinator)
                       .execute(statement, toApiCl(cl), bindings);
            }
        }
        catch (Throwable t)
        {
            // TODO: find a better way to work around timeouts
            if (t.getMessage().contains("timed out"))
                return execute(statement, cl, coordinator, bindings);

            logger.error(String.format("Caught error while trying execute statement %s (%s): %s",
                                       statement, Arrays.toString(bindings), t.getMessage()),
                         t);
            throw t;
        }
    }

    // TODO: Ideally, we need to be able to induce a failure of a single specific message
    public Object[][] executeWithWriteFailure(String statement, ConsistencyLevel cl, Object... bindings)
    {
        if (isShutdown.get())
            throw new RuntimeException("Instance is shut down");

        try
        {
            int coordinator = (int) (cnt.getAndIncrement() % cluster.size() + 1);
            IMessageFilters filters = cluster.filters();

            // Drop exactly one coordinated message
            int MUTATION_REQ = 0;
            // TODO: make dropping deterministic
            filters.verbs(MUTATION_REQ).from(coordinator).messagesMatching(new IMessageFilters.Matcher()
            {
                private final AtomicBoolean issued = new AtomicBoolean();
                public boolean matches(int from, int to, IMessage message)
                {
                    if (from != coordinator || message.verb() != MUTATION_REQ)
                        return false;

                    return !issued.getAndSet(true);
                }
            }).drop().on();
            Object[][] res = cluster
                             .coordinator(coordinator)
                             .execute(statement, toApiCl(cl), bindings);
            filters.reset();
            return res;
        }
        catch (Throwable t)
        {
            logger.error(String.format("Caught error while trying execute statement %s", statement),
                         t);
            throw t;
        }
    }

    public CompletableFuture<Object[][]> executeAsync(String statement, ConsistencyLevel cl, Object... bindings)
    {
        return CompletableFuture.supplyAsync(() -> execute(statement, cl, bindings), executor);
    }

    public CompletableFuture<Object[][]> executeAsyncWithWriteFailure(String statement, ConsistencyLevel cl, Object... bindings)
    {
        return CompletableFuture.supplyAsync(() -> executeWithWriteFailure(statement, cl, bindings), executor);
    }

    public static abstract class InJvmSutBaseConfiguration<NODE extends IInstance, CLUSTER extends ICluster<NODE>> implements Configuration.SutConfiguration
    {
        public final int nodes;
        public final int worker_threads;
        public final String root;

        @JsonCreator
        public InJvmSutBaseConfiguration(@JsonProperty(value = "nodes", defaultValue = "3") int nodes,
                                         @JsonProperty(value = "worker_threads", defaultValue = "10") int worker_threads,
                                         @JsonProperty("root") String root)
        {
            this.nodes = nodes;
            this.worker_threads = worker_threads;
            if (root == null)
            {
                try
                {
                    this.root = Files.createTempDirectory("cluster_" + nodes + "_nodes").toString();
                }
                catch (IOException e)
                {
                    throw new IllegalArgumentException(e);
                }
            }
            else
            {
                this.root = root;
            }
        }

        protected abstract CLUSTER cluster(Consumer<IInstanceConfig> cfg, int nodes, File root);
        protected abstract InJvmSutBase<NODE, CLUSTER> sut(CLUSTER cluster);

        public SystemUnderTest make()
        {
            try
            {
                ICluster.setup();
            }
            catch (Throwable throwable)
            {
                throw new RuntimeException(throwable);
            }

            CLUSTER cluster;

            cluster = cluster((cfg) -> {
                                  // TODO: make this configurable
                                  cfg.with(Feature.NETWORK, Feature.GOSSIP, Feature.NATIVE_PROTOCOL)
                                     .set("row_cache_size_in_mb", 10L)
                                     .set("index_summary_capacity_in_mb", 10L)
                                     .set("counter_cache_size_in_mb", 10L)
                                     .set("key_cache_size_in_mb", 10L)
                                     .set("file_cache_size_in_mb", 10)
                                     .set("memtable_heap_space_in_mb", 128)
                                     .set("memtable_offheap_space_in_mb", 128)
                                     .set("memtable_flush_writers", 1)
                                     .set("concurrent_compactors", 1)
                                     .set("concurrent_reads", 5)
                                     .set("concurrent_writes", 5)
                                     .set("compaction_throughput_mb_per_sec", 10)
                                     .set("hinted_handoff_enabled", false);
                              },
                              nodes,
                              new File(root));

            cluster.startup();
            return sut(cluster);
        }
    }

    public static org.apache.cassandra.distributed.api.ConsistencyLevel toApiCl(ConsistencyLevel cl)
    {
        switch (cl)
        {
            case ALL:    return org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
            case QUORUM: return org.apache.cassandra.distributed.api.ConsistencyLevel.QUORUM;
            case NODE_LOCAL: return org.apache.cassandra.distributed.api.ConsistencyLevel.NODE_LOCAL;
            case ONE: return org.apache.cassandra.distributed.api.ConsistencyLevel.ONE;
        }
        throw new IllegalArgumentException("Don't know a CL: " + cl);
    }
}
