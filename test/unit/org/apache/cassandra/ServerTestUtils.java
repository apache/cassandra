/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.audit.AuditLogManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.big.BigTableReader;
import org.apache.cassandra.io.sstable.indexsummary.IndexSummarySupport;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.locator.AbstractEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.security.ThreadAwareSecurityManager;
import org.apache.cassandra.service.EmbeddedCassandraService;

/**
 * Utility methodes used by SchemaLoader and CQLTester to manage the server and its state.
 *
 */
public final class ServerTestUtils
{
    private static final Logger logger = LoggerFactory.getLogger(ServerTestUtils.class);

    private static final Set<InetAddressAndPort> remoteAddrs = new HashSet<>();

    public static final String DATA_CENTER = "datacenter1";
    public static final String DATA_CENTER_REMOTE = "datacenter2";
    public static final String RACK1 = "rack1";

    private static boolean isServerPrepared = false;

    /**
     * Call DatabaseDescriptor.daemonInitialization ensuring that the snitch used returns fixed values for the tests
     */
    public static void daemonInitialization()
    {
        DatabaseDescriptor.daemonInitialization();

        // Register an EndpointSnitch which returns fixed values for test.
        DatabaseDescriptor.setEndpointSnitch(new AbstractEndpointSnitch()
        {
            @Override
            public String getRack(InetAddressAndPort endpoint)
            {
                return RACK1;
            }

            @Override
            public String getDatacenter(InetAddressAndPort endpoint)
            {
                if (remoteAddrs.contains(endpoint))
                    return DATA_CENTER_REMOTE;

                return DATA_CENTER;
            }

            @Override
            public int compareEndpoints(InetAddressAndPort target, Replica a1, Replica a2)
            {
                return 0;
            }
        });
    }

    public static void prepareServer()
    {
        if (isServerPrepared)
            return;

        DatabaseDescriptor.setTransientReplicationEnabledUnsafe(true);

        // Cleanup first
        try
        {
            cleanupAndLeaveDirs();
        }
        catch (IOException e)
        {
            logger.error("Failed to cleanup and recreate directories.");
            throw new RuntimeException(e);
        }

        try
        {
            remoteAddrs.add(InetAddressAndPort.getByName("127.0.0.4"));
        }
        catch (UnknownHostException e)
        {
            logger.error("Failed to lookup host");
            throw new RuntimeException(e);
        }

        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler()
        {
            public void uncaughtException(Thread t, Throwable e)
            {
                logger.error("Fatal exception in thread " + t, e);
            }
        });

        ThreadAwareSecurityManager.install();

        Keyspace.setInitialized();
        SystemKeyspace.persistLocalMetadata();
        AuditLogManager.instance.initialize();
        isServerPrepared = true;
    }

    /**
     * Cleanup the directories used by the server, creating them if they do not exist.
     */
    public static void cleanupAndLeaveDirs() throws IOException
    {
        CommitLog.instance.stopUnsafe(true);
        mkdirs(); // Creates the directories if they does not exists
        cleanup(); // Ensure that the directories are all empty
        CommitLog.instance.restartUnsafe();
    }

    /**
     * Cleanup the storage related directories: commitLog, cdc, hint, caches and data directories
     */
    public static void cleanup()
    {
        // clean up commitlog
        cleanupDirectory(DatabaseDescriptor.getCommitLogLocation());

        String cdcDir = DatabaseDescriptor.getCDCLogLocation();
        if (cdcDir != null)
            cleanupDirectory(cdcDir);
        cleanupDirectory(DatabaseDescriptor.getHintsDirectory());
        cleanupSavedCaches();

        // clean up data directory which are stored as data directory/keyspace/data files
        for (String dirName : DatabaseDescriptor.getAllDataFileLocations())
        {
            cleanupDirectory(dirName);
        }
    }

    private static void cleanupDirectory(File directory)
    {
        if (directory.exists())
        {
            Arrays.stream(directory.tryList()).forEach(File::deleteRecursive);
        }
    }

    private static void cleanupDirectory(String dirName)
    {
        if (dirName != null)
            cleanupDirectory(new File(dirName));
    }

    /**
     * Creates all the storage related directories
     */
    public static void mkdirs()
    {
        DatabaseDescriptor.createAllDirectories();
    }

    public static void cleanupSavedCaches()
    {
        cleanupDirectory(DatabaseDescriptor.getSavedCachesLocation());
    }

    public static EmbeddedCassandraService startEmbeddedCassandraService() throws IOException
    {
        DatabaseDescriptor.daemonInitialization();
        mkdirs();
        cleanup();
        EmbeddedCassandraService service = new EmbeddedCassandraService();
        service.start();
        return service;
    }

    private ServerTestUtils()
    {
    }

    public static List<BigTableReader> getLiveBigTableReaders(ColumnFamilyStore cfs)
    {
        return cfs.getLiveSSTables()
                  .stream()
                  .filter(BigTableReader.class::isInstance)
                  .map(BigTableReader.class::cast)
                  .collect(Collectors.toList());
    }

    public static <R extends SSTableReader & IndexSummarySupport<R>> List<R> getLiveIndexSummarySupportingReaders(ColumnFamilyStore cfs)
    {
        return cfs.getLiveSSTables()
                  .stream()
                  .filter(IndexSummarySupport.class::isInstance)
                  .map(r -> (R) r)
                  .collect(Collectors.toList());
    }
}
