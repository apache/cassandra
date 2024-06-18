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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.audit.AuditLogManager;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.big.BigTableReader;
import org.apache.cassandra.io.sstable.indexsummary.IndexSummarySupport;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.BaseProximity;
import org.apache.cassandra.security.ThreadAwareSecurityManager;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.cassandra.tcm.AtomicLongBackedProcessor;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Commit;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.MetadataSnapshots;
import org.apache.cassandra.tcm.Processor;
import org.apache.cassandra.tcm.RegistrationStatus;
import org.apache.cassandra.tcm.log.LocalLog;
import org.apache.cassandra.tcm.log.LogStorage;
import org.apache.cassandra.tcm.log.SystemKeyspaceStorage;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.ownership.PlacementProvider;
import org.apache.cassandra.tcm.ownership.UniformRangePlacement;
import org.apache.cassandra.tcm.transformations.ForceSnapshot;
import org.apache.cassandra.tcm.transformations.Register;
import org.apache.cassandra.tcm.transformations.UnsafeJoin;
import org.apache.cassandra.tcm.transformations.cms.Initialize;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.config.CassandraRelevantProperties.ORG_APACHE_CASSANDRA_DISABLE_MBEAN_REGISTRATION;

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
        initSnitch();
    }

    public static void initSnitch()
    {
        // Register an EndpointSnitch which returns fixed values for test.
        DatabaseDescriptor.setNodeProximity(new BaseProximity()
        {
            @Override
            public int compareEndpoints(InetAddressAndPort target, Replica a1, Replica a2)
            {
                return 0;
            }
        });
    }

    public static NodeId registerLocal()
    {
        return registerLocal(Collections.singleton(DatabaseDescriptor.getPartitioner().getRandomToken()));
    }

    public static NodeId registerLocal(Set<Token> tokens)
    {
        NodeId nodeId = Register.maybeRegister();
        ClusterMetadataService.instance().commit(new UnsafeJoin(nodeId,
                                                                tokens,
                                                                ClusterMetadataService.instance().placementProvider()));
        SystemKeyspace.setLocalHostId(nodeId.toUUID());
        RegistrationStatus.instance.onRegistration();
        return nodeId;
    }

    public static void prepareServer()
    {
        prepareServerNoRegister();
        registerLocal();
        markCMS();
    }

    public static void prepareServerNoRegister()
    {
        daemonInitialization();

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

        CassandraRelevantProperties.GOSSIPER_SKIP_WAITING_TO_SETTLE.setInt(0);
        initCMS();
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

    public static void initCMS()
    {
        // Effectively disable automatic snapshots using AtomicLongBackedProcessor and LocaLLog.Sync interacts
        // badly with submitting SealPeriod transformations from the log listener. In this configuration, SealPeriod
        // commits performed on NonPeriodicTasks threads end up actually performing the transformations as well as
        // calling the pre and post commit listeners, which is not threadsafe. In a non-test setup the processing of
        // log entries is always done by the dedicated log follower thread.
        DatabaseDescriptor.setMetadataSnapshotFrequency(Integer.MAX_VALUE);

        Function<LocalLog, Processor> processorFactory = AtomicLongBackedProcessor::new;
        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        Location location = DatabaseDescriptor.getLocator().local();
        boolean addListeners = true;
        ClusterMetadata initial = new ClusterMetadata(partitioner);
        if (!Keyspace.isInitialized())
            Keyspace.setInitialized();

        LocalLog log = LocalLog.logSpec()
                               .withInitialState(initial)
                               .withDefaultListeners(addListeners)
                               .createLog();

        ResettableClusterMetadataService service = new ResettableClusterMetadataService(new UniformRangePlacement(),
                                                                                        MetadataSnapshots.NO_OP,
                                                                                        log,
                                                                                        processorFactory.apply(log),
                                                                                        Commit.Replicator.NO_OP,
                                                                                        true);

        ClusterMetadataService.setInstance(service);
        log.readyUnchecked();
        log.bootstrap(FBUtilities.getBroadcastAddressAndPort(), location.datacenter);
        service.commit(new Initialize(ClusterMetadata.current()));
        QueryProcessor.registerStatementInvalidatingListener();
        service.mark();
    }

    public static void recreateCMS()
    {
        assert ORG_APACHE_CASSANDRA_DISABLE_MBEAN_REGISTRATION.getBoolean() : "Need to set " + ORG_APACHE_CASSANDRA_DISABLE_MBEAN_REGISTRATION + " to true for resetCMS to work";
        // unfortunately, for now this is sometimes necessary because of the initialisation ordering with regard to
        // IPartitioner. For example, if a test has a requirement to use a different partitioner to the one in yaml:
        // SchemaLoader.prepareServer
        // |-- SchemaLoader.prepareServerNoRegister
        // |   |-- ServerTestUtils.daemonInitialization();        # sets DD.partitioner according to yaml (i.e. BOP)
        // |   |-- ServerTestUtils.prepareServer();               # includes inititial CMS using DD partitioner
        // |-- StorageService.instance.setPartitionerUnsafe(M3P)  # test wants to use LongToken
        // |-- ServerTestUtils.recreateCMS                        # recreates the CMS using the updated partitioner
        ClusterMetadata initial = new ClusterMetadata(DatabaseDescriptor.getPartitioner());
        LogStorage storage = LogStorage.SystemKeyspace;
        LocalLog.LogSpec logSpec = LocalLog.logSpec()
                                           .withInitialState(initial)
                                           .withStorage(storage)
                                           .withDefaultListeners();
        LocalLog log = logSpec.createLog();

        ResettableClusterMetadataService cms = new ResettableClusterMetadataService(new UniformRangePlacement(),
                                                                                    MetadataSnapshots.NO_OP,
                                                                                    log,
                                                                                    new AtomicLongBackedProcessor(log),
                                                                                    Commit.Replicator.NO_OP,
                                                                                    true);
        ClusterMetadataService.unsetInstance();
        ClusterMetadataService.setInstance(cms);
        ((SystemKeyspaceStorage)LogStorage.SystemKeyspace).truncate();
        log.readyUnchecked();
        log.unsafeBootstrapForTesting(FBUtilities.getBroadcastAddressAndPort());
        cms.mark();
    }

    public static void markCMS()
    {
        ClusterMetadataService cms = ClusterMetadataService.instance();
        assert cms instanceof ResettableClusterMetadataService : "CMS instance is not resettable";
        ((ResettableClusterMetadataService)cms).mark();
    }

    public static void resetCMS()
    {
        ClusterMetadataService cms = ClusterMetadataService.instance();
        assert cms instanceof ResettableClusterMetadataService : "CMS instance is not resettable";
        ((ResettableClusterMetadataService)cms).reset();
    }

    public static class ResettableClusterMetadataService extends ClusterMetadataService
    {

        private ClusterMetadata mark;

        public ResettableClusterMetadataService(PlacementProvider placementProvider,
                                                MetadataSnapshots snapshots,
                                                LocalLog log,
                                                Processor processor,
                                                Commit.Replicator replicator,
                                                boolean isMemberOfOwnershipGroup)
        {
            super(placementProvider, snapshots, log, processor, replicator, isMemberOfOwnershipGroup);
            mark = log.metadata();
        }

        public void mark()
        {
            mark = log().metadata();
        }

        public Epoch reset()
        {
            Epoch nextEpoch = ClusterMetadata.current().epoch.nextEpoch();
            ClusterMetadata newBaseState = mark.forceEpoch(nextEpoch);
            return ClusterMetadataService.instance().commit(new ForceSnapshot(newBaseState)).epoch;
        }
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
