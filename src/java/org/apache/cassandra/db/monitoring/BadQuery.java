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
package org.apache.cassandra.db.monitoring;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.Attributes;
import org.apache.cassandra.db.*;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.MonitoringService;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;

/**
 * Collect and log queries qualified as bad query.
 */
public class BadQuery
{
    @VisibleForTesting
    public enum BadQueryCategory
    {
        SLOW_READ_LOCAL("slow local read"),
        SLOW_READ_COORINATOR("slow coordinator read"),
        SLOW_WRITE_LOCAL("slow local write"),
        SLOW_WRITE_COORINATOR("slow coordinator write"),
        LARGE_PARTITION_READ("large partition read"),
        LARGE_PARTITION_WRITE("large partition write"),
        INCORRECT_COMPACTION_STRATEBY("incorrect compaction strategy"),
        INCORRECT_CONSISTENCY_LEVEL("incorrect consistency level"),
        TOO_MANY_TOMBSTONES("too many tombstones"),
        MV_IN_USE("mv is in use"),
        PREPARED_CACHE_OVERFLOW("too many prepared statements");

        private final String text;

        BadQueryCategory(String text)
        {
            this.text = text;
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            sb.append("policyid:");
            sb.append(ordinal());
            sb.append(", ");
            sb.append(text);
            return sb.toString();
        }
    }

    public static void setup()
    {
        DatabaseDescriptor.getBadQueryReporter().setup();

        MonitoringService.instance.setBadQueryMaxSamplesInSyslog(DatabaseDescriptor.getBadQueryMaxSamplesPerIntervalInSyslog());
        MonitoringService.instance.setBadQueryTracingFraction(DatabaseDescriptor.getBadQueryTracingFraction());
        MonitoringService.instance.setBadQueryReadMaxPartitionSizeInbytes(DatabaseDescriptor.getBadQueryReadMaxPartitionsizeInBytes());
        MonitoringService.instance.setBadQueryWriteMaxPartitionSizeInbytes(DatabaseDescriptor.getBadQueryWriteMaxPartitionSizeInBytes());
        MonitoringService.instance.setBadQueryReadSlowLocalLatencyInms(DatabaseDescriptor.getBadQueryReadSlowLocalLatencyInMs());
        MonitoringService.instance.setBadQueryWriteSlowLocalLatencyInms(DatabaseDescriptor.getBadQueryWriteSlowLocalLatencyInMs());
        MonitoringService.instance.setBadQueryReadSlowCoordLatencyInms(DatabaseDescriptor.getBadQueryReadSlowCoordLatenchInMs());
        MonitoringService.instance.setBadQueryWriteSlowCoordLatencyInms(DatabaseDescriptor.getBadQueryWriteSlowCoordLatencyInMs());
        MonitoringService.instance.setBadQueryTombstoneLimit(DatabaseDescriptor.getBadQueryTombstoneLimit());
        if (!DatabaseDescriptor.getBadQueryIgnoreKeyspaces().isEmpty()) {
            MonitoringService.instance.setBadQueryIgnoreKeyspacesPattern(Pattern.compile(DatabaseDescriptor
                                                                                  .getBadQueryIgnoreKeyspaces()));
        }

    }

    /**
     * report bad query to reporter.
     *
     * @param queryType badquery category.
     * @param operation operation category.
     */
    public static void report(BadQueryCategory queryType, BadQueryTypes operation)
    {
        DatabaseDescriptor.getBadQueryReporter().reportBadQuery(queryType, operation);
    }

    /**
     * decides if tracing should happen for given query or not
     * based on configuration, here are the three possible outcomes:
     * 1. trace all queries.
     * 2. tracing none of the queries.
     * 3. tracing percentage of queries.
     * User can change this behavior anytime w/o restarting daemon.
     *
     * @return true if current query needs to be traced, otherwise false.
     */
    public static boolean shouldTrace(String ksName)
    {
        if (!DatabaseDescriptor.isBadQueryTracingEnabled() ||
                (Double.compare(MonitoringService.instance.getBadQueryTracingFraction(), 0.0d) == 0) ||
                MonitoringService.instance.getBadQueryIgnoreKeyspaces().contains(ksName))
        {
            return false;
        }
        if (Double.compare(MonitoringService.instance.getBadQueryTracingFraction(), 1.0d) == 0)
        {
            return true;
        }
        return ThreadLocalRandom.current().nextDouble() <= MonitoringService.instance.getBadQueryTracingFraction();
    }

    /**
     * Check current query if it qualifies large partition read or not.
     *
     * @param readCommand actual read command.
     * @param size        size read.
     */
    public static void checkForLargeRead(ReadCommand readCommand,
                                         long size)
    {
        if (shouldTrace(readCommand.metadata().keyspace))
        {
            LargePartition.checkForLargeRead(readCommand, size);
        }
    }

    /**
     * Check current query if it qualifies slow coordinator write or not.
     *
     * @param mutations    actual mutation.
     * @param durationInns time it took to write mutations.
     */
    public static void checkForSlowCoordinatorWrite(Collection<? extends IMutation> mutations,
                                                    long durationInns)
    {
        for (IMutation mutation : mutations)
        {
            //for now even if a single keyspace is in ignore list then do not trace entire batch
            if (!shouldTrace(mutation.getKeyspaceName()))
            {
                return;
            }
        }
        SlowQuery.checkForSlowCoordinatorWrite(mutations, durationInns);
    }

    /**
     * Check current query if it qualifies large write or not.
     *
     * @param mutation actual mutation.
     * @param size     size of mutation.
     */
    public static void checkForLargeWrite(Mutation mutation,
                                          long size)
    {
        if (shouldTrace(mutation.getKeyspaceName()))
        {
            LargePartition.checkForLargeWrite(mutation, size);
        }
    }

    /**
     * Check current query if it qualifies large write or not.
     *
     * @param metadata TableMetadata of the cfs.
     * @param decoratedKey partition key in DecoratedKey.
     * @param size     size of mutation.
     */
    public static void checkForLargeWrite(TableMetadata metadata,
                                          DecoratedKey decoratedKey,
                                          long size)
    {
        if (shouldTrace(metadata.keyspace))
        {
            LargePartition.checkForLargeWrite(metadata, decoratedKey, size);
        }
    }

    /**
     * Check current query if it qualifies slow local read or not.
     *
     * @param command      read command.
     * @param durationInns time it took to read query.
     */
    public static void checkForSlowLocalRead(ReadCommand command,
                                             long durationInns)
    {
        if (shouldTrace(command.metadata().keyspace))
        {
            SlowQuery.checkForSlowLocalRead(command, durationInns);
        }
    }

    /**
     * Check current query if it qualifies slow coordinator read or not.
     *
     * @param commands    read commands.
     * @param latencyinns time it took to read all the commands.
     */
    public static void checkForSlowCoordinatorRead(List<SinglePartitionReadCommand> commands,
                                                   long latencyinns)
    {
        for (ReadCommand command : commands)
        {
            //for now even if a single keyspace is in ignore list then do not trace entire batch
            if (!shouldTrace(command.metadata().keyspace))
            {
                return;
            }
        }
        SlowQuery.checkForSlowCoordinatorRead(commands, latencyinns);
    }

    /**
     * Check current query if it qualifies for too many tombstones or not.
     *
     * @param command    read command.
     * @param tombstones tombstones count.
     */
    public static void checkForTooManyTombstones(ReadCommand command,
                                                 int tombstones)
    {
        if (shouldTrace(command.metadata().keyspace))
        {
            ToomanyTombstones.checkForTooManyTombstones(command, tombstones);
        }
    }

    /**
     * Check if table compaction strategy is correct or not.
     *
     * @param tableMetadata   table metadata.
     * @param attrs query attributes.
     */
    public static void checkForCompactionStrategySettings(TableMetadata tableMetadata,
                                                          Attributes attrs)
    {
        if (shouldTrace(tableMetadata.keyspace))
        {
            InvalidConfiguration.checkForInvalidCompaction(tableMetadata, attrs);
        }
    }

    /**
     * Check if query consistency level is correct or not.
     *
     * @param tableMetadata table metadata.
     * @param cl  consistency level for query.
     */
    public static void checkForCLSettings(TableMetadata tableMetadata, ConsistencyLevel cl, boolean isWritePath)
    {
        if (shouldTrace(tableMetadata.keyspace))
        {
            InvalidConfiguration.checkForInvalidConsistency(tableMetadata, cl, isWritePath);
        }
    }

    /**
     * Check if MV is being used or not. MV has been marked an experimental
     * feature in Open Source Cassandra.
     *
     * @param cfm table metadata.
     */
    public static void checkForMV(TableMetadata cfm)
    {
        if (shouldTrace(cfm.keyspace))
        {
            MvInUse.checkForMV(cfm);
        }
    }

    /**
     * Check if there are any prepared statement cache evictions.
     *
     * @param count the number of evicted prepared statements from cache in the last minute.
     */
    public static void checkForPreparedCacheOverflow(long count)
    {
        PreparedCacheOverflow.checkForPreparedCacheOverflow(count);
    }
}
