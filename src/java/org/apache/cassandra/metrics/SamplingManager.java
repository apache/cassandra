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

package org.apache.cassandra.metrics;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.tools.nodetool.ProfileLoad;
import org.apache.cassandra.tools.nodetool.formatter.TableBuilder;
import org.apache.cassandra.utils.Pair;

public class SamplingManager
{
    private static final Logger logger = LoggerFactory.getLogger(SamplingManager.class);

    /**
     * Tracks the active scheduled sampling tasks.
     * The key of the map is a {@link JobId}, which is effectively a keyspace + table abstracted behind some syntactic
     * sugar so we can use them without peppering Pairs throughout this class. Both keyspace and table are nullable,
     * a paradigm we inherit from {@link ProfileLoad} so need to accommodate here.
     *
     * The value of the map is the current scheduled task.
     */
    private final ConcurrentHashMap<JobId, Future<?>> activeSamplingTasks = new ConcurrentHashMap<>();

    /** Tasks that are actively being cancelled */
    private final Set<JobId> cancelingTasks = ConcurrentHashMap.newKeySet();

    public static String formatResult(ResultBuilder resultBuilder)
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (PrintStream ps = new PrintStream(baos))
        {
            for (Sampler.SamplerType samplerType : Sampler.SamplerType.values())
            {
                samplerType.format(resultBuilder, ps);
            }
            return baos.toString();
        }
    }

    public static Iterable<ColumnFamilyStore> getTables(String ks, String table)
    {
        // null KEYSPACE == all the tables
        if (ks == null)
            return ColumnFamilyStore.all();

        Keyspace keyspace = Keyspace.open(ks);

        // KEYSPACE defined w/null table == all the tables on that KEYSPACE
        if (table == null)
            return keyspace.getColumnFamilyStores();
        // Or we just have a specific ks+table combo we're looking to profile
        else
            return Collections.singletonList(keyspace.getColumnFamilyStore(table));
    }

    /**
     * Register the samplers for the keyspace and table.
     * @param ks Keyspace. Nullable. If null, the scheduled sampling is on all keyspaces and tables
     * @param table Nullable. If null, the scheduled sampling is on all tables of the specified keyspace
     * @param duration Duration of each scheduled sampling job in milliseconds
     * @param interval Interval of each scheduled sampling job in milliseconds
     * @param capacity Capacity of the sampler, higher for more accuracy
     * @param count Number of the top samples to list
     * @param samplers a list of samplers to enable
     * @return true if the scheduled sampling is started successfully. Otherwise return fasle
     */
    public boolean register(String ks, String table, int duration, int interval, int capacity, int count, List<String> samplers)
    {
        JobId jobId = new JobId(ks, table);
        logger.info("Registering samplers {} for {}", samplers, jobId);

        if (!canSchedule(jobId))
        {
            logger.info("Unable to register {} due to existing ongoing sampling.", jobId);
            return false;
        }

        // 'begin' tasks are chained to finish before their paired 'finish'
        activeSamplingTasks.put(jobId, ScheduledExecutors.optionalTasks.submit(
        createSamplingBeginRunnable(jobId, getTables(ks, table), duration, interval, capacity, count, samplers)
        ));
        return true;
    }

    public boolean unregister(String ks, String table)
    {
        // unregister all
        // return true when all tasks are cancelled successfully
        if (ks == null && table == null)
        {
            boolean res = true;
            for (JobId id : activeSamplingTasks.keySet())
            {
                res = cancelTask(id) & res;
            }
            return res;
        }
        else
        {
            return cancelTask(new JobId(ks, table));
        }
    }

    public List<String> allJobs()
    {
        return jobIds().stream()
                       .map(JobId::toString)
                       .collect(Collectors.toList());
    }

    private Set<JobId> jobIds()
    {
        Set<JobId> all = new HashSet<>();
        all.addAll(activeSamplingTasks.keySet());
        all.addAll(cancelingTasks);
        return all;
    }

    /**
     * Validate if a schedule on the keyspace and table is permitted
     * @param jobId
     * @return true if possible, false if there are overlapping tables already being sampled
     */
    private boolean canSchedule(JobId jobId)
    {
        Set<JobId> allJobIds = jobIds();
        // There is a schedule that works on all tables. Overlapping guaranteed.
        if (allJobIds.contains(JobId.ALL_KS_AND_TABLES) || (!allJobIds.isEmpty() && jobId.equals(JobId.ALL_KS_AND_TABLES)))
            return false;
        // there is an exactly duplicated schedule
        else if (allJobIds.contains(jobId))
            return false;
        else
            // make sure has no overlapping tables under the keyspace
            return !allJobIds.contains(JobId.createForAllTables(jobId.keyspace));
    }

    /**
     * Cancel a task by its id. The corresponding task will be stopped once its final sampling completes.
     * @param jobId
     * @return true if the task exists, false if not found
     */
    private boolean cancelTask(JobId jobId)
    {
        Future<?> task = activeSamplingTasks.remove(jobId);
        if (task != null)
            cancelingTasks.add(jobId);
        return task != null;
    }

    /**
     * Begin sampling and schedule a future task to end the sampling task
     */
    private Runnable createSamplingBeginRunnable(JobId jobId, Iterable<ColumnFamilyStore> tables, int duration, int interval, int capacity, int count, List<String> samplers)
    {
        return () ->
        {
            List<String> tableNames = StreamSupport.stream(tables.spliterator(), false)
                                                   .map(cfs -> String.format("%s.%s", cfs.keyspace, cfs.name))
                                                   .collect(Collectors.toList());
            logger.info("Starting to sample tables {} with the samplers {} for {} ms", tableNames, samplers, duration);
            for (String sampler : samplers)
            {
                for (ColumnFamilyStore cfs : tables)
                {
                    cfs.beginLocalSampling(sampler, capacity, duration);
                }
            }
            Future<?> fut = ScheduledExecutors.optionalTasks.schedule(
                createSamplingEndRunnable(jobId, tables, duration, interval, capacity, count, samplers),
                interval,
                TimeUnit.MILLISECONDS);
            // reached to the end of the current runnable
            // update the referenced future to SamplingFinish
            activeSamplingTasks.put(jobId, fut);
        };
    }

    /**
     * Finish the sampling and begin a new one immediately after.
     *
     * NOTE: Do not call this outside the context of {@link this#createSamplingBeginRunnable}, as we need to preserve
     * ordering between a "start" and "end" runnable
     */
    private Runnable createSamplingEndRunnable(JobId jobId, Iterable<ColumnFamilyStore> tables, int duration, int interval, int capacity, int count, List<String> samplers)
    {
        return () ->
        {
            Map<String, List<CompositeData>> results = new HashMap<>();
            for (String sampler : samplers)
            {
                List<CompositeData> topk = new ArrayList<>();
                for (ColumnFamilyStore cfs : tables)
                {
                    try
                    {
                        topk.addAll(cfs.finishLocalSampling(sampler, count));
                    }
                    catch (OpenDataException e)
                    {
                        logger.warn("Failed to retrieve the sampled data. Abort the background sampling job: {}.", jobId, e);
                        activeSamplingTasks.remove(jobId);
                        cancelingTasks.remove(jobId);
                        return;
                    }
                }

                topk.sort((left, right) -> Long.compare((long) right.get("count"), (long) left.get("count")));
                // sublist is not serializable for jmx
                topk = new ArrayList<>(topk.subList(0, Math.min(topk.size(), count)));
                results.put(sampler, topk);
            }
            AtomicBoolean first = new AtomicBoolean(false);
            ResultBuilder rb = new ResultBuilder(first, results, samplers);
            logger.info(formatResult(rb));

            // If nobody has canceled us, we ping-pong back to a "begin" runnable to run another profile load
            if (!cancelingTasks.contains(jobId))
            {
                Future<?> fut = ScheduledExecutors.optionalTasks.submit(
                    createSamplingBeginRunnable(jobId, tables, duration, interval, capacity, count, samplers));
                activeSamplingTasks.put(jobId, fut);
            }
            // If someone *has* canceled us, we need to remove the runnable from activeSampling and also remove the
            // cancellation sentinel so subsequent re-submits of profiling don't get blocked immediately
            else
            {
                logger.info("The sampling job {} has been cancelled.", jobId);
                activeSamplingTasks.remove(jobId);
                cancelingTasks.remove(jobId);
            }
        };
    }

    private static class JobId
    {
        public static final JobId ALL_KS_AND_TABLES = new JobId(null, null);

        public final String keyspace;
        public final String table;

        public JobId(String ks, String tb)
        {
            keyspace = ks;
            table = tb;
        }

        public static JobId createForAllTables(String keyspace)
        {
            return new JobId(keyspace, null);
        }

        @Override
        public String toString()
        {
            return maybeWildCard(keyspace) + '.' + maybeWildCard(table);
        }

        private String maybeWildCard(String input)
        {
            return input == null ? "*" : input;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            JobId jobId = (JobId) o;
            return Objects.equals(keyspace, jobId.keyspace) && Objects.equals(table, jobId.table);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(keyspace, table);
        }
    }

    public static class ResultBuilder
    {
        protected Sampler.SamplerType type;
        protected String description;
        protected AtomicBoolean first;
        protected Map<String, List<CompositeData>> results;
        protected List<String> targets;
        protected List<Pair<String, String>> dataKeys;

        public ResultBuilder(AtomicBoolean first, Map<String, List<CompositeData>> results, List<String> targets)
        {
            this.first = first;
            this.results = results;
            this.targets = targets;
            this.dataKeys = new ArrayList<>();
            this.dataKeys.add(Pair.create("  ", "  "));
        }

        public SamplingManager.ResultBuilder forType(Sampler.SamplerType type, String description)
        {
            SamplingManager.ResultBuilder rb = new SamplingManager.ResultBuilder(first, results, targets);
            rb.type = type;
            rb.description = description;
            return rb;
        }

        public SamplingManager.ResultBuilder addColumn(String title, String key)
        {
            this.dataKeys.add(Pair.create(title, key));
            return this;
        }

        protected String get(CompositeData cd, String key)
        {
            if (cd.containsKey(key))
                return cd.get(key).toString();
            return key;
        }

        public void print(PrintStream ps)
        {
            if (targets.contains(type.toString()))
            {
                if (!first.get())
                    ps.println();
                first.set(false);
                ps.println(description + ':');
                TableBuilder out = new TableBuilder();
                out.add(dataKeys.stream().map(p -> p.left).collect(Collectors.toList()).toArray(new String[] {}));
                List<CompositeData> topk = results.get(type.toString());
                for (CompositeData cd : topk)
                {
                    out.add(dataKeys.stream().map(p -> get(cd, p.right)).collect(Collectors.toList()).toArray(new String[] {}));
                }
                if (topk.size() == 0)
                {
                    ps.println("   Nothing recorded during sampling period...");
                }
                else
                {
                    out.printTo(ps);
                }
            }
        }
    }
}
