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
import java.nio.ByteBuffer;
import java.nio.file.FileStore;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Range;
import com.vdurmont.semver4j.Semver;

import net.jpountz.lz4.LZ4Factory;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.JMXServerOptions;
import org.apache.cassandra.config.StartupChecksConfiguration;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.StartupException;
import org.apache.cassandra.io.compress.AbstractCompressionProvider;
import org.apache.cassandra.io.compress.CompressorRegistry;
import org.apache.cassandra.io.compress.ICompressor;
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

import static org.apache.cassandra.config.CassandraRelevantProperties.COM_SUN_MANAGEMENT_JMXREMOTE_PORT;
import static org.apache.cassandra.config.CassandraRelevantProperties.IGNORE_KERNEL_BUG_1057843_CHECK;
import static org.apache.cassandra.config.CassandraRelevantProperties.JAVA_VERSION;
import static org.apache.cassandra.config.CassandraRelevantProperties.JAVA_VM_NAME;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;
import static org.apache.cassandra.utils.LocalizeString.toLowerCaseLocalized;

/**
 * Verifies that the system and environment is in a fit state to be started.
 * Used in CassandraDaemon#setup() to check various settings and invariants.
 *
 * Each individual test is modelled as an implementation of StartupCheck, these are run
 * at the start of CassandraDaemon#setup() before any local state is mutated. The default
 * checks are a mix of informational tests (inspectJvmOptions), initialization
 * (checkProcessEnvironment, checkCacheServiceInitialization) and invariant checking
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
    private static final Logger logger = LoggerFactory.getLogger(StartupChecks.class);
    // List of checks to run before starting up. If any test reports failure, startup will be halted.
    private final List<StartupCheck> preFlightChecks = new ArrayList<>();

    // The default set of pre-flight checks to run. Order is somewhat significant in that we probably
    // always want the system keyspace check run last, as this actually loads the schema for that
    // keyspace. All other checks should not require any schema initialization.
    private final List<StartupCheck> DEFAULT_TESTS = ImmutableList.of(checkKernelBug1057843,
                                                                      checkJemalloc,
                                                                      checkLz4Native,
                                                                      checkValidLaunchDate,
                                                                      checkJMXPorts,
                                                                      checkJMXProperties,
                                                                      inspectJvmOptions,
                                                                      checkNativeLibraryInitialization,
                                                                      checkProcessEnvironment,
                                                                      checkMaxMapCount,
                                                                      checkReadAheadKbSetting,
                                                                      checkDataDirs,
                                                                      checkDirectIOSupport,
                                                                      checkSSTablesFormat,
                                                                      checkSystemKeyspaceState,
                                                                      checkLegacyAuthTables,
                                                                      checkKernelParamsForAsyncProfiler,
                                                                      checkCustomCompressionProviders,
                                                                      new DataResurrectionCheck());

    public List<StartupCheck> getChecks()
    {
        return List.copyOf(preFlightChecks);
    }

    public StartupCheck getCheck(String name)
    {
        for (StartupCheck startupCheck : preFlightChecks)
        {
            if (startupCheck.name().equals(name))
                return startupCheck;
        }
        return null;
    }

    public StartupChecks withDefaultTests()
    {
        preFlightChecks.addAll(DEFAULT_TESTS);
        return this;
    }

    public StartupChecks withServiceLoaderTests()
    {
        Set<StartupCheck> customChecks = new HashSet<>();
        Set<String> uniqueNames = new HashSet<>();
        Set<String> duplicitNames = new HashSet<>();

        try
        {
            for (StartupCheck check : ServiceLoader.load(StartupCheck.class))
            {
                if (!uniqueNames.add(check.name()))
                    duplicitNames.add(check.name());
                else
                    customChecks.add(check);
            }
        }
        catch (ServiceConfigurationError t)
        {
            throw new ConfigurationException("Unable to get startup checks via ServiceLoader. " +
                                             "Custom checks will not be triggered.", t);
        }

        if (!duplicitNames.isEmpty())
        {
            throw new IllegalStateException("There was an attempt to load custom startup " +
                                            "checks with same name which is ambiguous: " + duplicitNames);
        }

        for (StartupCheck customCheck : customChecks)
        {
            for (StartupCheck preFlightCheck : preFlightChecks)
            {
                if (preFlightCheck.name().equals(customCheck.name()))
                {
                    throw new IllegalStateException("There was an attempt to load custom startup check " +
                                                    "with same name as in-built check: " + preFlightCheck.name());
                }
            }
        }

        preFlightChecks.addAll(customChecks);

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
    public void verify(StartupChecksConfiguration options) throws StartupException
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
                logger.warn("Failed to run startup check post-action on " + test.name());
            }
        }
    }

    // https://bugs.debian.org/cgi-bin/bugreport.cgi?bug=1057843
    public static final StartupCheck checkKernelBug1057843 = new StartupCheck()
    {
        @Override
        public String name()
        {
            return "kernel_bug_1057843";
        }

        @Override
        public void execute(StartupChecksConfiguration configuration) throws StartupException
        {
            if (configuration.isDisabled(name()))
                return;

            if (!FBUtilities.isLinux)
                return;

            Set<Path> directIOWritePaths = new HashSet<>();
            if (DatabaseDescriptor.getCommitLogWriteDiskAccessMode() == Config.DiskAccessMode.direct)
                directIOWritePaths.add(new File(DatabaseDescriptor.getCommitLogLocation()).toPath());
            // Note: Data directories for direct IO compaction reads are checked in checkDirectIOSupport.
            // This check is specifically for direct IO writes which are currently only supported for commit log.

            if (!directIOWritePaths.isEmpty() && IGNORE_KERNEL_BUG_1057843_CHECK.getBoolean())
            {
                logger.info("Ignoring check for the kernel bug 1057843 against the following paths configured to be accessed with Direct IO: {}", directIOWritePaths);
                return;
            }

            Set<String> affectedFileSystemTypes = Set.of("ext4");
            Set<Path> affectedPaths = new HashSet<>();
            for (Path path : directIOWritePaths)
            {
                try
                {
                    if (affectedFileSystemTypes.contains(toLowerCaseLocalized(Files.getFileStore(path).type())))
                        affectedPaths.add(path);
                }
                catch (IOException e)
                {
                    throw new StartupException(StartupException.ERR_WRONG_MACHINE_STATE, "Failed to determine file system type for path " + path, e);
                }
            }

            if (affectedPaths.isEmpty())
                return;

            Range<Semver> affectedKernels = Range.closedOpen(new Semver("6.1.64", Semver.SemverType.LOOSE),
                                                             new Semver("6.1.66", Semver.SemverType.LOOSE));

            Semver kernelVersion = FBUtilities.getKernelVersion();
            if (!affectedKernels.contains(kernelVersion.withClearedSuffixAndBuild()))
                return;

            throw new StartupException(StartupException.ERR_WRONG_MACHINE_STATE,
                                       String.format("Detected kernel version %s with affected file system types %s and direct IO enabled for paths %s. " +
                                                     "This combination is known to cause data corruption. To start Cassandra in this environment, " +
                                                     "you have to disable direct IO for the affected paths. If you are sure the verification provided " +
                                                     "a false positive result, you can suppress it by setting '" + IGNORE_KERNEL_BUG_1057843_CHECK.getKey() + "' system property to 'true'. " +
                                                     "Please see https://bugs.debian.org/cgi-bin/bugreport.cgi?bug=1057843 for more information.",
                                                     kernelVersion, affectedFileSystemTypes, affectedPaths));
        }
    };

    public static final StartupCheck checkJemalloc = new StartupCheck()
    {
        @Override
        public String name()
        {
            return "jemalloc";
        }

        @Override
        public void execute(StartupChecksConfiguration configuration)
        {
            if (configuration.isDisabled(name()))
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
        public String name()
        {
            return "lz4_native";
        }

        @Override
        public void execute(StartupChecksConfiguration configuration)
        {
            if (configuration.isDisabled(name()))
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

    public static final StartupCheck checkCustomCompressionProviders = new StartupCheck()
    {
        @Override
        public String name()
        {
            return "custom_compression_providers";
        }

        @Override
        public void execute(StartupChecksConfiguration configuration) throws StartupException
        {
            if (configuration.isDisabled(name()))
                return;

            // Resolving the custom providers forces classloading (and native-lib init) of each
            // configured compressor; a missing class or failed native init is a configuration error.
            Map<Class<?>, AbstractCompressionProvider> providers = getCustomProviders();

            if (providers.isEmpty())
                return;

            long seed = (new Random()).nextLong();
            Random random = new Random(seed);
            byte[] payload = smokeTestPayload(random);

            logger.info("Running compression smoke test for {} custom provider(s) with seed {}. " +
                        "To reproduce a failure, regenerate the 4 KiB payload with new java.util.Random({}).",
                        providers.size(), seed, seed);

            List<String> failedProviders = new ArrayList<>();
            for (Map.Entry<Class<?>, AbstractCompressionProvider> entry : providers.entrySet())
            {
                Class<?> compressorClass = entry.getKey();
                AbstractCompressionProvider provider = entry.getValue();

                ICompressor custom;

                try
                {
                    custom = provider.createCompressor(compressorClass, Collections.emptyMap());
                }
                catch (Throwable t)
                {
                    throw new StartupException(StartupException.ERR_WRONG_CONFIG,
                                               String.format("Unable to instantiate a compressor for class %s " +
                                                             "for the purposes of a startup check from provider %s.",
                                                             compressorClass.getName(),
                                                             provider.getClass().getName()));
                }

                if (custom.serializedAs() != compressorClass)
                {
                    throw new StartupException(StartupException.ERR_WRONG_CONFIG,
                                               String.format("Provider %s returned a compressor whose serializedAs() is %s, " +
                                                             "but it must be %s (the built-in it substitutes for).",
                                                             provider.getClass().getName(),
                                                             custom.serializedAs(),
                                                             compressorClass.getName()));
                }

                ICompressor builtin = CompressorRegistry.DEFAULT_COMPRESSION_PROVIDER.createCompressor(compressorClass, Collections.emptyMap());

                try
                {
                    // Round trip both ways so the custom and built-in compressors are proven to share
                    // an on-disk-compatible format (peers/restarts without the plugin read the data).
                    assertCompatibleRoundTrip(custom, builtin, payload);
                    assertCompatibleRoundTrip(builtin, custom, payload);

                    logger.info("Compression smoke test passed for custom provider {} ({}).",
                                provider.getClass().getName(), compressorClass.getSimpleName());
                }
                catch (Throwable t)
                {
                    logger.error("Compression smoke test failed for custom provider {} ({}); reproduce with seed {}.",
                                 provider.getClass().getName(), compressorClass.getSimpleName(), seed, t);
                    failedProviders.add(provider.getClass().getName() + " -> " + compressorClass.getSimpleName());
                }
            }

            if (!failedProviders.isEmpty())
            {
                throw new StartupException(StartupException.ERR_WRONG_MACHINE_STATE,
                                           String.format("The following custom compression providers failed smoke test: %s. " +
                                                         "Providers substitute for a built-in compressor, so non byte-compatible output " +
                                                         "is silent data corruption when read without the provider. Reproduce with the seed %s.",
                                                         Joiner.on(", ").join(failedProviders),
                                                         seed));
            }
        }

        private Map<Class<?>, AbstractCompressionProvider> getCustomProviders() throws StartupException
        {
            Map<Class<?>, AbstractCompressionProvider> providers;
            try
            {
                providers = CompressorRegistry.instance.getCustomProviders();
            }
            catch (Throwable t)
            {
                throw new StartupException(StartupException.ERR_WRONG_CONFIG,
                                           "Failed to load configured custom compression providers; " +
                                           "check compressor_providers in cassandra.yaml", t);
            }
            return providers;
        }
        // Compresses payload with `compressor`, decompresses with `decompressor`, and verifies the
        // result matches - proving the two share an on-disk-compatible format. Throws on mismatch.
        private void assertCompatibleRoundTrip(ICompressor compressor, ICompressor decompressor, byte[] payload) throws IOException
        {
            ByteBuffer input = compressor.preferredBufferType().allocate(payload.length);
            int compressedLength = compressor.initialCompressedBufferLength(payload.length);
            ByteBuffer compressed = compressor.preferredBufferType().allocate(compressedLength);
            input.put(payload);
            input.flip();
            compressor.compress(input, compressed);
            compressed.flip();

            // Within a compress/uncompress call the in/out buffers must share a type, but the compressor
            // and decompressor may prefer different ones; only re-stage the compressed bytes in that case.
            if (compressor.preferredBufferType() != decompressor.preferredBufferType())
            {
                ByteBuffer staged = decompressor.preferredBufferType().allocate(compressed.remaining());
                staged.put(compressed);
                staged.flip();
                compressed = staged;
            }

            ByteBuffer output = decompressor.preferredBufferType().allocate(payload.length);
            decompressor.uncompress(compressed, output);
            output.flip();

            if (!output.equals(ByteBuffer.wrap(payload)))
            {
                throw new IOException(String.format("Round-trip mismatch: compressed with %s, decompressed with %s",
                                                    compressor.getClass().getName(), decompressor.getClass().getName()));
            }
        }
        private byte[] smokeTestPayload(Random random)
        {
            // 4 KiB payload: the first half zeros (highly compressible, exercises the real compression
            // path), the second half high-entropy bytes (incompressible, exercises the compressor's
            // worst-case output sizing via initialCompressedBufferLength).
            byte[] testPayload = new byte[4 * 1024];
            byte[] randomHalf = new byte[testPayload.length / 2];
            random.nextBytes(randomHalf);
            System.arraycopy(randomHalf, 0, testPayload, testPayload.length / 2, randomHalf.length);
            return testPayload;
        }
    };

    public static final StartupCheck checkValidLaunchDate = new StartupCheck()
    {
        @Override
        public String name()
        {
            return "valid_launch_date";
        }

        /**
         * The earliest legit timestamp a casandra instance could have ever launched.
         * Date roughly taken from http://perspectives.mvdirona.com/2008/07/12/FacebookReleasesCassandraAsOpenSource.aspx
         * We use this to ensure the system clock is at least somewhat correct at startup.
         */
        private static final long EARLIEST_LAUNCH_DATE = 1215820800000L;

        @Override
        public void execute(StartupChecksConfiguration configuration) throws StartupException
        {
            if (configuration.isDisabled(name()))
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
        public String name()
        {
            return "jmx_ports";
        }

        @Override
        public void execute(StartupChecksConfiguration configuration)
        {
            if (configuration.isDisabled(name()))
                return;

            JMXServerOptions jmxServerOptions = DatabaseDescriptor.getJmxServerOptions();
            if (!jmxServerOptions.enabled)
            {
                logger.warn("JMX connection server is not enabled for either local or remote connections. " +
                            "Please see jmx_server_options in cassandra.yaml for more info");
            }
            if (!jmxServerOptions.remote)
            {
                logger.warn("JMX is not enabled to receive remote connections. " +
                            "Please see jmx_server_options in cassandra.yaml for more info.");
            }
            else
            {
                logger.info("JMX is enabled to receive remote connections on port: {}", jmxServerOptions.jmx_port);
            }
        }
    };

    public static final StartupCheck checkJMXProperties = new StartupCheck()
    {
        @Override
        public String name()
        {
            return "jmx_properties";
        }

        @Override
        public void execute(StartupChecksConfiguration configuration)
        {
            if (configuration.isDisabled(name()))
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
        public String name()
        {
            return "jvm_options";
        }

        @Override
        public void execute(StartupChecksConfiguration configuration)
        {
            if (configuration.isDisabled(name()))
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
        public String name()
        {
            return "native_library_initialization";
        }

        @Override
        public void execute(StartupChecksConfiguration configuration) throws StartupException
        {
            if (configuration.isDisabled(name()))
                return;
            // Fail-fast if the native library could not be linked.
            if (!NativeLibrary.isAvailable())
                throw new StartupException(StartupException.ERR_WRONG_MACHINE_STATE, "The native library could not be initialized properly. ");
        }
    };

    public static final StartupCheck checkProcessEnvironment = new StartupCheck()
    {
        @Override
        public String name()
        {
            return "process_environment";
        }

        @Override
        public void execute(StartupChecksConfiguration configuration)
        {
            Optional<String> degradations = FBUtilities.getSystemInfo().isDegraded();

            if (degradations.isPresent())
                logger.warn("Cassandra server running in degraded mode. " + degradations.get());
            else
                logger.info("Checked OS settings and found them configured for optimal performance.");
        }
    };

    public static final StartupCheck checkReadAheadKbSetting = new StartupCheck()
    {
        @Override
        public String name()
        {
            return "read_ahead_kb_setting";
        }

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
        public void execute(StartupChecksConfiguration configuration)
        {
            if (configuration.isDisabled(name()) || !FBUtilities.isLinux)
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
        @Override
        public String name()
        {
            return "max_map_count";
        }

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
        public void execute(StartupChecksConfiguration configuration)
        {
            if (configuration.isDisabled(name()) || !FBUtilities.isLinux)
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
        public String name()
        {
            return "data_dirs";
        }

        @Override
        public void execute(StartupChecksConfiguration configuration) throws StartupException
        {
            if (configuration.isDisabled(name()))
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

    public static final StartupCheck checkDirectIOSupport = new StartupCheck()
    {
        @Override
        public String name()
        {
            return "directio_support";
        }

        @Override
        public void execute(StartupChecksConfiguration configuration) throws StartupException
        {
            if (configuration.isDisabled(name()))
                return;

            // Only check if compaction_read_disk_access_mode is direct
            if (DatabaseDescriptor.getCompactionReadDiskAccessMode() != Config.DiskAccessMode.direct)
                return;

            List<String> unsupportedLocations = findDirectIOUnsupportedLocations(DatabaseDescriptor.getAllDataFileLocations());

            if (!unsupportedLocations.isEmpty())
            {
                throw new StartupException(StartupException.ERR_WRONG_DISK_STATE,
                                           String.format("Direct I/O is configured for compaction reads (compaction_read_disk_access_mode=direct), " +
                                                         "but the following data directories do not support Direct I/O: %s. " +
                                                         "Either change compaction_read_disk_access_mode to 'standard' in cassandra.yaml, " +
                                                         "or ensure all data directories are on filesystems that support Direct I/O. " +
                                                         "Network filesystems (NFS, CIFS) and some virtual filesystems do not support Direct I/O.",
                                                         unsupportedLocations));
            }
        }
    };

    @VisibleForTesting
    static List<String> findDirectIOUnsupportedLocations(String[] dataFileLocations)
    {
        List<String> unsupportedLocations = new ArrayList<>();

        for (String dataDir : dataFileLocations)
        {
            File dir = new File(dataDir);
            if (!dir.exists())
                continue; // Directory doesn't exist yet, skip

            if (!FileUtils.isDirectIOSupported(dir))
                unsupportedLocations.add(dataDir);
        }

        return unsupportedLocations;
    }

    public static final StartupCheck checkSSTablesFormat = new StartupCheck()
    {
        @Override
        public String name()
        {
            return "sstables_format";
        }

        @Override
        public void execute(StartupChecksConfiguration configuration) throws StartupException
        {
            if (configuration.isDisabled(name()))
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
        public String name()
        {
            return "system_keyspace_state";
        }

        @Override
        public void execute(StartupChecksConfiguration configuration) throws StartupException
        {
            if (configuration.isDisabled(name()))
                return;
            // check the system keyspace to keep user from shooting self in foot by changing partitioner, cluster name, etc.
            // we do a one-off scrub of the system keyspace first; we can't load the list of the rest of the keyspaces,
            // until system keyspace is opened.

            for (TableMetadata cfm : Schema.instance.getTablesAndViews(SchemaConstants.SYSTEM_KEYSPACE_NAME))
                ColumnFamilyStore.scrubDataDirectories(cfm);

            if (DatabaseDescriptor.getAccordTransactionsEnabled())
            {
                for (TableMetadata cfm : Schema.instance.getTablesAndViews(SchemaConstants.ACCORD_KEYSPACE_NAME))
                    ColumnFamilyStore.scrubDataDirectories(cfm);
            }

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

    public static final StartupCheck checkLegacyAuthTables = new StartupCheck()
    {
        @Override
        public String name()
        {
            return "legacy_auth_tables";
        }

        @Override
        public void execute(StartupChecksConfiguration configuration) throws StartupException
        {
            if (configuration.isDisabled(name()))
                return;
            Optional<String> errMsg = checkLegacyAuthTablesMessage();
            if (errMsg.isPresent())
                throw new StartupException(StartupException.ERR_WRONG_CONFIG, errMsg.get());
        }
    };

    public static final StartupCheck checkKernelParamsForAsyncProfiler = new AsyncProfilerKernelParamsCheck();

    public static class AsyncProfilerKernelParamsCheck implements StartupCheck
    {
        private static final String MESSAGE = "Async-profiler experience likely affected. Kernel symbols are unavailable due to restrictions. " +
                                              "Try 'sysctl kernel.perf_event_paranoid=1' and 'sysctl kernel.kptr_restrict=0' or its " +
                                              "variation on your system to resolve the issue.";

        @VisibleForTesting
        public int readPerfEventParanoid()
        {
            List<String> lines = FileUtils.readLines(new File("/proc/sys/kernel/perf_event_paranoid"));
            if (!lines.isEmpty())
                return Integer.parseInt(lines.get(0));
            return Integer.MIN_VALUE;
        }

        @VisibleForTesting
        public int readKptrRestrict()
        {
            List<String> lines = FileUtils.readLines(new File("/proc/sys/kernel/kptr_restrict"));
            if (!lines.isEmpty())
                return Integer.parseInt(lines.get(0));
            return Integer.MIN_VALUE;
        }

        public boolean hasCorrectKernelParams()
        {
            int perfEventParanoid = readPerfEventParanoid();
            int kptrRestrict = readKptrRestrict();

            return perfEventParanoid <= 1 && kptrRestrict == 0;
        }


        @Override
        public String name()
        {
            return "async_profiler_kernel_parameters";
        }

        public void execute(StartupChecksConfiguration startupChecksConfiguration, boolean shouldThrow)
        {
            try
            {
                if (!CassandraRelevantProperties.ASYNC_PROFILER_ENABLED.getBoolean())
                    return;

                int perfEventParanoid = readPerfEventParanoid();
                int kptrRestrict = readKptrRestrict();

                if (perfEventParanoid == Integer.MIN_VALUE || kptrRestrict == Integer.MIN_VALUE)
                {
                    logger.debug("Unable to determine values for kernel parameter of " +
                                 "'kernel.perf_event_paranoid' and 'kernel.kptr_restrict' for Async-profiler. " +
                                 "Its usability might be limited.");
                }
                else if (perfEventParanoid > 1 || kptrRestrict != 0)
                {
                    if (shouldThrow)
                        throw new IllegalStateException(MESSAGE);
                    else
                        logger.warn(MESSAGE);
                }
            }
            catch (Throwable t)
            {
                if (shouldThrow)
                    throw t;
            }
        }

        @Override
        public void execute(StartupChecksConfiguration configuration)
        {
            execute(configuration, false);
        }
    }

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
