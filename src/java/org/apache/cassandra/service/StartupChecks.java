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
package org.apache.cassandra.service;

import java.io.BufferedReader;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.nio.file.FileStore;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jpountz.lz4.LZ4Factory;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.StartupChecksOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.StartupException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.UUIDBasedSSTableId;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.PathUtils;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JavaUtils;
import org.apache.cassandra.utils.NativeLibrary;
import org.apache.cassandra.utils.SigarLibrary;

import static org.apache.cassandra.config.CassandraRelevantProperties.CASSANDRA_JMX_LOCAL_PORT;
import static org.apache.cassandra.config.CassandraRelevantProperties.COM_SUN_MANAGEMENT_JMXREMOTE_PORT;
import static org.apache.cassandra.config.CassandraRelevantProperties.JAVA_VERSION;
import static org.apache.cassandra.config.CassandraRelevantProperties.JAVA_VM_NAME;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;

/**
 * Verifies that the system and environment is in a fit state to be started.
 * Used in CassandraDaemon#setup() to check various settings and invariants.
 *
 * Each individual test is modelled as an implementation of StartupCheck, these are run
 * at the start of CassandraDaemon#setup() before any local state is mutated. The default
 * checks are a mix of informational tests (inspectJvmOptions), initialization
 * (initSigarLibrary, checkCacheServiceInitialization) and invariant checking
 * (checkValidLaunchDate, checkSystemKeyspaceState, checkSSTablesFormat).
 *
 * In addition, if checkSystemKeyspaceState determines that the release version has
 * changed since last startup (i.e. the node has been upgraded) it snapshots the system
 * keyspace to make it easier to back out if necessary.
 *
 * If any check reports a failure, then the setup method exits with an error (after
 * logging any output from the tests). If all tests report success, setup can continue.
 * We should be careful in future to ensure anything which mutates local state (such as
 * writing new sstables etc) only happens after we've verified the initial setup.
 */
public class StartupChecks
{
    public enum StartupCheckType
    {
        // non-configurable check is always enabled for execution
        non_configurable_check,
        check_filesystem_ownership(true),
        check_dc,
        check_rack,
        check_data_resurrection(true);

        public final boolean disabledByDefault;

        StartupCheckType()
        {
            this(false);
        }

        StartupCheckType(boolean disabledByDefault)
        {
            this.disabledByDefault = disabledByDefault;
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(StartupChecks.class);
    // List of checks to run before starting up. If any test reports failure, startup will be halted.
    private final List<StartupCheck> preFlightChecks = new ArrayList<>();

    // The default set of pre-flight checks to run. Order is somewhat significant in that we probably
    // always want the system keyspace check run last, as this actually loads the schema for that
    // keyspace. All other checks should not require any schema initialization.
    private final List<StartupCheck> DEFAULT_TESTS = ImmutableList.of(checkJemalloc,
                                                                      checkLz4Native,
                                                                      checkValidLaunchDate,
                                                                      checkJMXPorts,
                                                                      checkJMXProperties,
                                                                      inspectJvmOptions,
                                                                      checkNativeLibraryInitialization,
                                                                      initSigarLibrary,
                                                                      checkMaxMapCount,
                                                                      checkReadAheadKbSetting,
                                                                      checkDataDirs,
                                                                      checkSSTablesFormat,
                                                                      checkSystemKeyspaceState,
                                                                      checkDatacenter,
                                                                      checkRack,
                                                                      checkLegacyAuthTables,
                                                                      new DataResurrectionCheck());

    public StartupChecks withDefaultTests()
    {
        preFlightChecks.addAll(DEFAULT_TESTS);
        return this;
    }

    /**
     * Add system test to be run before schema is loaded during startup
     * @param test the system test to include
     */
    public StartupChecks withTest(StartupCheck test)
    {
        preFlightChecks.add(test);
        return this;
    }

    /**
     * Run the configured tests and return a report detailing the results.
     * @throws StartupException if any test determines that the
     * system is not in an valid state to startup
     * @param options options to pass to respective checks for their configration
     */
    public void verify(StartupChecksOptions options) throws StartupException
    {
        for (StartupCheck test : preFlightChecks)
            test.execute(options);

        for (StartupCheck test : preFlightChecks)
        {
            try
            {
                test.postAction(options);
            }
            catch (Throwable t)
            {
                logger.warn("Failed to run startup check post-action on " + test.getStartupCheckType());
            }
        }
    }

    public static final StartupCheck checkJemalloc = new StartupCheck()
    {
        @Override
        public void execute(StartupChecksOptions options)
        {
            if (options.isDisabled(getStartupCheckType()))
                return;

            String jemalloc = CassandraRelevantProperties.LIBJEMALLOC.getString();
            if (jemalloc == null)
                logger.warn("jemalloc shared library could not be preloaded to speed up memory allocations");
            else if ("-".equals(jemalloc))
                logger.info("jemalloc preload explicitly disabled");
            else
                logger.info("jemalloc seems to be preloaded from {}", jemalloc);
        }
    };

    public static final StartupCheck checkLz4Native = new StartupCheck()
    {
        @Override
        public void execute(StartupChecksOptions options)
        {
            if (options.isDisabled(getStartupCheckType()))
                return;
            try
            {
                LZ4Factory.nativeInstance(); // make sure native loads
            }
            catch (AssertionError | LinkageError e)
            {
                logger.warn("lz4-java was unable to load native libraries; this will lower the performance of lz4 (network/sstables/etc.): {}", Throwables.getRootCause(e).getMessage());
            }
        }
    };

    public static final StartupCheck checkValidLaunchDate = new StartupCheck()
    {
        /**
         * The earliest legit timestamp a casandra instance could have ever launched.
         * Date roughly taken from http://perspectives.mvdirona.com/2008/07/12/FacebookReleasesCassandraAsOpenSource.aspx
         * We use this to ensure the system clock is at least somewhat correct at startup.
         */
        private static final long EARLIEST_LAUNCH_DATE = 1215820800000L;

        @Override
        public void execute(StartupChecksOptions options) throws StartupException
        {
            if (options.isDisabled(getStartupCheckType()))
                return;
            long now = currentTimeMillis();
            if (now < EARLIEST_LAUNCH_DATE)
                throw new StartupException(StartupException.ERR_WRONG_MACHINE_STATE,
                                           String.format("current machine time is %s, but that is seemingly incorrect. exiting now.",
                                                         new Date(now).toString()));
        }
    };

    public static final StartupCheck checkJMXPorts = new StartupCheck()
    {
        @Override
        public void execute(StartupChecksOptions options)
        {
            if (options.isDisabled(getStartupCheckType()))
                return;
            String jmxPort = CassandraRelevantProperties.CASSANDRA_JMX_REMOTE_PORT.getString();
            if (jmxPort == null)
            {
                logger.warn("JMX is not enabled to receive remote connections. Please see cassandra-env.sh for more info.");
                jmxPort = CassandraRelevantProperties.CASSANDRA_JMX_LOCAL_PORT.toString();
                if (jmxPort == null)
                    logger.error(CASSANDRA_JMX_LOCAL_PORT.getKey() + " missing from cassandra-env.sh, unable to start local JMX service.");
            }
            else
            {
                logger.info("JMX is enabled to receive remote connections on port: {}", jmxPort);
            }
        }
    };

    public static final StartupCheck checkJMXProperties = new StartupCheck()
    {
        @Override
        public void execute(StartupChecksOptions options)
        {
            if (options.isDisabled(getStartupCheckType()))
                return;
            if (COM_SUN_MANAGEMENT_JMXREMOTE_PORT.isPresent())
            {
                logger.warn("Use of com.sun.management.jmxremote.port at startup is deprecated. " +
                            "Please use cassandra.jmx.remote.port instead.");
            }
        }
    };

    public static final StartupCheck inspectJvmOptions = new StartupCheck()
    {
        @Override
        public void execute(StartupChecksOptions options)
        {
            if (options.isDisabled(getStartupCheckType()))
                return;
            // log warnings for different kinds of sub-optimal JVMs.  tldr use 64-bit Oracle >= 1.6u32
            if (!DatabaseDescriptor.hasLargeAddressSpace())
                logger.warn("32bit JVM detected.  It is recommended to run Cassandra on a 64bit JVM for better performance.");

            String javaVmName = JAVA_VM_NAME.getString();
            if (!(javaVmName.contains("HotSpot") || javaVmName.contains("OpenJDK")))
            {
                logger.warn("Non-Oracle JVM detected.  Some features, such as immediate unmap of compacted SSTables, may not work as intended");
            }
            else
            {
                checkOutOfMemoryHandling();
            }
        }

        /**
         * Checks that the JVM is configured to handle OutOfMemoryError
         */
        private void checkOutOfMemoryHandling()
        {
            if (JavaUtils.supportExitOnOutOfMemory(JAVA_VERSION.getString()))
            {
                if (!jvmOptionsContainsOneOf("-XX:OnOutOfMemoryError=", "-XX:+ExitOnOutOfMemoryError", "-XX:+CrashOnOutOfMemoryError"))
                    logger.warn("The JVM is not configured to stop on OutOfMemoryError which can cause data corruption."
                                + " Use one of the following JVM options to configure the behavior on OutOfMemoryError: "
                                + " -XX:+ExitOnOutOfMemoryError, -XX:+CrashOnOutOfMemoryError, or -XX:OnOutOfMemoryError=\"<cmd args>;<cmd args>\"");
            }
            else
            {
                if (!jvmOptionsContainsOneOf("-XX:OnOutOfMemoryError="))
                    logger.warn("The JVM is not configured to stop on OutOfMemoryError which can cause data corruption."
                            + " Either upgrade your JRE to a version greater or equal to 8u92 and use -XX:+ExitOnOutOfMemoryError/-XX:+CrashOnOutOfMemoryError"
                            + " or use -XX:OnOutOfMemoryError=\"<cmd args>;<cmd args>\" on your current JRE.");
            }
        }

        /**
         * Checks if one of the specified options is being used.
         * @param optionNames The name of the options to check
         * @return {@code true} if one of the specified options is being used, {@code false} otherwise.
         */
        private boolean jvmOptionsContainsOneOf(String... optionNames)
        {
            RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
            List<String> inputArguments = runtimeMxBean.getInputArguments();
            for (String argument : inputArguments)
            {
                for (String optionName : optionNames)
                    if (argument.startsWith(optionName))
                        return true;
            }
            return false;
        }
    };

    public static final StartupCheck checkNativeLibraryInitialization = new StartupCheck()
    {
        @Override
        public void execute(StartupChecksOptions options) throws StartupException
        {
            if (options.isDisabled(getStartupCheckType()))
                return;
            // Fail-fast if the native library could not be linked.
            if (!NativeLibrary.isAvailable())
                throw new StartupException(StartupException.ERR_WRONG_MACHINE_STATE, "The native library could not be initialized properly. ");
        }
    };

    public static final StartupCheck initSigarLibrary = new StartupCheck()
    {
        @Override
        public void execute(StartupChecksOptions options)
        {
            if (options.isDisabled(getStartupCheckType()))
                return;
            SigarLibrary.instance.warnIfRunningInDegradedMode();
        }
    };

    public static final StartupCheck checkReadAheadKbSetting = new StartupCheck()
    {
        // This value is in KB.
        private static final long MAX_RECOMMENDED_READ_AHEAD_KB_SETTING = 128;

        /**
         * Function to get the block device system path(Example: /dev/sda) from the
         * data directories defined in cassandra config.(cassandra.yaml)
         * @param dataDirectories list of data directories from cassandra.yaml
         * @return Map of block device path and data directory
         */
        private Map<String, String> getBlockDevices(String[] dataDirectories) {
            Map<String, String> blockDevices = new HashMap<String, String>();

            for (String dataDirectory : dataDirectories)
            {
                try
                {
                    Path p = File.getPath(dataDirectory);
                    FileStore fs = Files.getFileStore(p);

                    String blockDirectory = fs.name();
                    if(StringUtils.isNotEmpty(blockDirectory))
                    {
                        blockDevices.put(blockDirectory, dataDirectory);
                    }
                }
                catch (IOException e)
                {
                    logger.warn("IO exception while reading file {}.", dataDirectory, e);
                }
            }
            return blockDevices;
        }

        @Override
        public void execute(StartupChecksOptions options)
        {
            if (options.isDisabled(getStartupCheckType()) || !FBUtilities.isLinux)
                return;

            String[] dataDirectories = DatabaseDescriptor.getRawConfig().data_file_directories;
            Map<String, String> blockDevices = getBlockDevices(dataDirectories);

            for (Map.Entry<String, String> entry: blockDevices.entrySet())
            {
                String blockDeviceDirectory = entry.getKey();
                String dataDirectory = entry.getValue();
                try
                {
                    Path readAheadKBPath = StartupChecks.getReadAheadKBPath(blockDeviceDirectory);

                    if (readAheadKBPath == null || Files.notExists(readAheadKBPath))
                    {
                        logger.debug("No 'read_ahead_kb' setting found for device {} of data directory {}.", blockDeviceDirectory, dataDirectory);
                        continue;
                    }

                    final List<String> data = Files.readAllLines(readAheadKBPath);
                    if (data.isEmpty())
                        continue;

                    int readAheadKbSetting = Integer.parseInt(data.get(0));

                    if (readAheadKbSetting > MAX_RECOMMENDED_READ_AHEAD_KB_SETTING)
                    {
                        logger.warn("Detected high '{}' setting of {} for device '{}' of data directory '{}'. It is " +
                                    "recommended to set this value to 8KB (or lower) on SSDs or 64KB (or lower) on HDDs " +
                                    "to prevent excessive IO usage and page cache churn on read-intensive workloads.",
                                    readAheadKBPath, readAheadKbSetting, blockDeviceDirectory, dataDirectory);
                    }
                }
                catch (final IOException e)
                {
                    logger.warn("IO exception while reading file {}.", blockDeviceDirectory, e);
                }
            }
        }
    };

    public static final StartupCheck checkMaxMapCount = new StartupCheck()
    {
        private final long EXPECTED_MAX_MAP_COUNT = 1048575;
        private final String MAX_MAP_COUNT_PATH = "/proc/sys/vm/max_map_count";

        private long getMaxMapCount()
        {
            final Path path = File.getPath(MAX_MAP_COUNT_PATH);
            try (final BufferedReader bufferedReader = Files.newBufferedReader(path))
            {
                final String data = bufferedReader.readLine();
                if (data != null)
                {
                    try
                    {
                        return Long.parseLong(data);
                    }
                    catch (final NumberFormatException e)
                    {
                        logger.warn("Unable to parse {}.", path, e);
                    }
                }
            }
            catch (final IOException e)
            {
                logger.warn("IO exception while reading file {}.", path, e);
            }
            return -1;
        }

        @Override
        public void execute(StartupChecksOptions options)
        {
            if (options.isDisabled(getStartupCheckType()) || !FBUtilities.isLinux)
                return;

            if (DatabaseDescriptor.getDiskAccessMode() == Config.DiskAccessMode.standard &&
                DatabaseDescriptor.getIndexAccessMode() == Config.DiskAccessMode.standard)
                return; // no need to check if disk access mode is only standard and not mmap

            long maxMapCount = getMaxMapCount();
            if (maxMapCount < EXPECTED_MAX_MAP_COUNT)
                logger.warn("Maximum number of memory map areas per process (vm.max_map_count) {} " +
                            "is too low, recommended value: {}, you can change it with sysctl.",
                            maxMapCount, EXPECTED_MAX_MAP_COUNT);
        }
    };

    public static final StartupCheck checkDataDirs = new StartupCheck()
    {
        @Override
        public void execute(StartupChecksOptions options) throws StartupException
        {
            if (options.isDisabled(getStartupCheckType()))
                return;
            // check all directories(data, commitlog, saved cache) for existence and permission
            Iterable<String> dirs = Iterables.concat(Arrays.asList(DatabaseDescriptor.getAllDataFileLocations()),
                                                     Arrays.asList(DatabaseDescriptor.getCommitLogLocation(),
                                                                   DatabaseDescriptor.getSavedCachesLocation(),
                                                                   DatabaseDescriptor.getHintsDirectory().absolutePath()));
            for (String dataDir : dirs)
            {
                logger.debug("Checking directory {}", dataDir);
                File dir = new File(dataDir);

                // check that directories exist.
                if (!dir.exists())
                {
                    logger.warn("Directory {} doesn't exist", dataDir);
                    // if they don't, failing their creation, stop cassandra.
                    if (!dir.tryCreateDirectories())
                        throw new StartupException(StartupException.ERR_WRONG_DISK_STATE,
                                                   "Has no permission to create directory "+ dataDir);
                }

                // if directories exist verify their permissions
                if (!Directories.verifyFullPermissions(dir, dataDir))
                    throw new StartupException(StartupException.ERR_WRONG_DISK_STATE,
                                               "Insufficient permissions on directory " + dataDir);
            }
        }
    };

    public static final StartupCheck checkSSTablesFormat = new StartupCheck()
    {
        @Override
        public void execute(StartupChecksOptions options) throws StartupException
        {
            if (options.isDisabled(getStartupCheckType()))
                return;
            final Set<String> invalid = new HashSet<>();
            final Set<String> nonSSTablePaths = new HashSet<>();
            final List<String> withIllegalGenId = new ArrayList<>();
            nonSSTablePaths.add(FileUtils.getCanonicalPath(DatabaseDescriptor.getCommitLogLocation()));
            nonSSTablePaths.add(FileUtils.getCanonicalPath(DatabaseDescriptor.getSavedCachesLocation()));
            nonSSTablePaths.add(FileUtils.getCanonicalPath(DatabaseDescriptor.getHintsDirectory()));

            FileVisitor<Path> sstableVisitor = new SimpleFileVisitor<Path>()
            {
                public FileVisitResult visitFile(Path path, BasicFileAttributes attrs)
                {
                    File file = new File(path);
                    if (!Descriptor.isValidFile(file))
                        return FileVisitResult.CONTINUE;

                    try
                    {
                        Descriptor desc = Descriptor.fromFileWithComponent(file, false).left;
                        if (!desc.isCompatible())
                            invalid.add(file.toString());

                        if (!DatabaseDescriptor.isUUIDSSTableIdentifiersEnabled() && desc.id instanceof UUIDBasedSSTableId)
                            withIllegalGenId.add(file.toString());
                    }
                    catch (Exception e)
                    {
                        invalid.add(file.toString());
                    }
                    return FileVisitResult.CONTINUE;
                }

                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException
                {
                    String[] nameParts = FileUtils.getCanonicalPath(new File(dir)).split(java.io.File.separator);
                    if (nameParts.length >= 2)
                    {
                        String tablePart = nameParts[nameParts.length - 1];
                        String ksPart = nameParts[nameParts.length - 2];

                        if (tablePart.contains("-"))
                            tablePart = tablePart.split("-")[0];

                        // In very old versions of Cassandra, we wouldn't necessarily delete sstables from dropped system tables
                        // which were removed in various major version upgrades (e.g system.Versions in 1.2)
                        if (ksPart.equals(SchemaConstants.SYSTEM_KEYSPACE_NAME) && !SystemKeyspace.ALL_TABLE_NAMES.contains(tablePart))
                        {
                            String canonicalPath = FileUtils.getCanonicalPath(new File(dir));

                            // We can have snapshots of our system tables or snapshots created with a -t tag of "system" that would trigger
                            // this potential warning, so we warn more softly in the case that it's probably a snapshot.
                            if (canonicalPath.contains("snapshot"))
                            {
                                logger.info("Found unknown system directory {}.{} at {} that contains the word snapshot. " +
                                            "This may be left over from a previous version of Cassandra or may be normal. " +
                                            " Consider removing after inspection if determined to be unnecessary.",
                                            ksPart, tablePart, canonicalPath);
                            }
                            else
                            {
                                logger.warn("Found unknown system directory {}.{} at {} - this is likely left over from a previous " +
                                            "version of Cassandra and should be removed after inspection.",
                                            ksPart, tablePart, canonicalPath);
                            }
                            return FileVisitResult.SKIP_SUBTREE;
                        }
                    }

                    String name = dir.getFileName().toString();
                    return (name.equals(Directories.SNAPSHOT_SUBDIR)
                            || name.equals(Directories.BACKUPS_SUBDIR)
                            || nonSSTablePaths.contains(PathUtils.toCanonicalPath(dir).toString()))
                           ? FileVisitResult.SKIP_SUBTREE
                           : FileVisitResult.CONTINUE;
                }
            };

            for (String dataDir : DatabaseDescriptor.getAllDataFileLocations())
            {
                try
                {
                    Files.walkFileTree(new File(dataDir).toPath(), sstableVisitor);
                }
                catch (IOException e)
                {
                    throw new StartupException(3, "Unable to verify sstable files on disk", e);
                }
            }

            if (!invalid.isEmpty())
                throw new StartupException(StartupException.ERR_WRONG_DISK_STATE,
                                           String.format("Detected unreadable sstables %s, please check " +
                                                         "NEWS.txt and ensure that you have upgraded through " +
                                                         "all required intermediate versions, running " +
                                                         "upgradesstables",
                                                         Joiner.on(",").join(invalid)));

            if (!withIllegalGenId.isEmpty())
                throw new StartupException(StartupException.ERR_WRONG_CONFIG,
                                           "UUID sstable identifiers are disabled but some sstables have been " +
                                           "created with UUID identifiers. You have to either delete those " +
                                           "sstables or enable UUID based sstable identifers in cassandra.yaml " +
                                           "(uuid_sstable_identifiers_enabled). The list of affected sstables is: " +
                                           Joiner.on(", ").join(withIllegalGenId) + ". If you decide to delete sstables, " +
                                           "and have that data replicated over other healthy nodes, those will be brought" +
                                           "back during repair");
        }
    };

    public static final StartupCheck checkSystemKeyspaceState = new StartupCheck()
    {
        @Override
        public void execute(StartupChecksOptions options) throws StartupException
        {
            if (options.isDisabled(getStartupCheckType()))
                return;
            // check the system keyspace to keep user from shooting self in foot by changing partitioner, cluster name, etc.
            // we do a one-off scrub of the system keyspace first; we can't load the list of the rest of the keyspaces,
            // until system keyspace is opened.

            for (TableMetadata cfm : Schema.instance.getTablesAndViews(SchemaConstants.SYSTEM_KEYSPACE_NAME))
                ColumnFamilyStore.scrubDataDirectories(cfm);

            try
            {
                SystemKeyspace.checkHealth();
            }
            catch (ConfigurationException e)
            {
                throw new StartupException(StartupException.ERR_WRONG_CONFIG, "Fatal exception during initialization", e);
            }
        }
    };

    public static final StartupCheck checkDatacenter = new StartupCheck()
    {
        @Override
        public void execute(StartupChecksOptions options) throws StartupException
        {
            boolean enabled = options.isEnabled(getStartupCheckType());
            if (CassandraRelevantProperties.IGNORE_DC.isPresent())
            {
                logger.warn(String.format("Cassandra system property flag %s is deprecated and you should " +
                                          "use startup check configuration in cassandra.yaml",
                                          CassandraRelevantProperties.IGNORE_DC.getKey()));
                enabled = !CassandraRelevantProperties.IGNORE_DC.getBoolean();
            }
            if (enabled)
            {
                String storedDc = SystemKeyspace.getDatacenter();
                if (storedDc != null)
                {
                    String currentDc = DatabaseDescriptor.getEndpointSnitch().getLocalDatacenter();
                    if (!storedDc.equals(currentDc))
                    {
                        String formatMessage = "Cannot start node if snitch's data center (%s) differs from previous data center (%s). " +
                                               "Please fix the snitch configuration, decommission and rebootstrap this node or use the flag -Dcassandra.ignore_dc=true.";

                        throw new StartupException(StartupException.ERR_WRONG_CONFIG, String.format(formatMessage, currentDc, storedDc));
                    }
                }
            }
        }

        @Override
        public StartupCheckType getStartupCheckType()
        {
            return StartupCheckType.check_dc;
        }
    };

    public static final StartupCheck checkRack = new StartupCheck()
    {
        @Override
        public void execute(StartupChecksOptions options) throws StartupException
        {
            boolean enabled = options.isEnabled(getStartupCheckType());
            if (CassandraRelevantProperties.IGNORE_RACK.isPresent())
            {
                logger.warn(String.format("Cassandra system property flag %s is deprecated and you should " +
                                          "use startup check configuration in cassandra.yaml",
                                          CassandraRelevantProperties.IGNORE_RACK.getKey()));
                enabled = !CassandraRelevantProperties.IGNORE_RACK.getBoolean();
            }
            if (enabled)
            {
                String storedRack = SystemKeyspace.getRack();
                if (storedRack != null)
                {
                    String currentRack = DatabaseDescriptor.getEndpointSnitch().getLocalRack();
                    if (!storedRack.equals(currentRack))
                    {
                        String formatMessage = "Cannot start node if snitch's rack (%s) differs from previous rack (%s). " +
                                               "Please fix the snitch configuration, decommission and rebootstrap this node or use the flag -Dcassandra.ignore_rack=true.";

                        throw new StartupException(StartupException.ERR_WRONG_CONFIG, String.format(formatMessage, currentRack, storedRack));
                    }
                }
            }
        }

        @Override
        public StartupCheckType getStartupCheckType()
        {
            return StartupCheckType.check_rack;
        }
    };

    public static final StartupCheck checkLegacyAuthTables = new StartupCheck()
    {
        @Override
        public void execute(StartupChecksOptions options) throws StartupException
        {
            if (options.isDisabled(getStartupCheckType()))
                return;
            Optional<String> errMsg = checkLegacyAuthTablesMessage();
            if (errMsg.isPresent())
                throw new StartupException(StartupException.ERR_WRONG_CONFIG, errMsg.get());
        }
    };

    @VisibleForTesting
    public static Path getReadAheadKBPath(String blockDirectoryPath)
    {
        Path readAheadKBPath = null;

        final String READ_AHEAD_KB_SETTING_PATH = "/sys/block/%s/queue/read_ahead_kb";
        try
        {
            String[] blockDirComponents = blockDirectoryPath.split("/");
            if (blockDirComponents.length >= 2 && blockDirComponents[1].equals("dev"))
            {
                String deviceName = blockDirComponents[2].replaceAll("[0-9]*$", "");
                if (StringUtils.isNotEmpty(deviceName))
                {
                    readAheadKBPath = File.getPath(String.format(READ_AHEAD_KB_SETTING_PATH, deviceName));
                }
            }
        }
        catch (Exception e)
        {
            logger.error("Error retrieving device path for {}.", blockDirectoryPath);
        }

        return readAheadKBPath;
    }

    @VisibleForTesting
    static Optional<String> checkLegacyAuthTablesMessage()
    {
        List<String> existing = new ArrayList<>(SchemaConstants.LEGACY_AUTH_TABLES).stream().filter((legacyAuthTable) ->
            {
                UntypedResultSet result = QueryProcessor.executeOnceInternal(String.format("SELECT table_name FROM %s.%s WHERE keyspace_name='%s' AND table_name='%s'",
                                                                                           SchemaConstants.SCHEMA_KEYSPACE_NAME,
                                                                                           "tables",
                                                                                           SchemaConstants.AUTH_KEYSPACE_NAME,
                                                                                           legacyAuthTable));
                return result != null && !result.isEmpty();
            }).collect(Collectors.toList());

        if (!existing.isEmpty())
            return Optional.of(String.format("Legacy auth tables %s in keyspace %s still exist and have not been properly migrated.",
                        Joiner.on(", ").join(existing), SchemaConstants.AUTH_KEYSPACE_NAME));
        else
            return Optional.empty();
    };
}
