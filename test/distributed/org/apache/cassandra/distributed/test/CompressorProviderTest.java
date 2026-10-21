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

package org.apache.cassandra.distributed.test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IIsolatedExecutor.SerializableCallable;
import org.apache.cassandra.io.compress.AbstractCompressionProvider;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.compress.CompressorRegistry;
import org.apache.cassandra.io.compress.DefaultCompressionProvider;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.compress.LZ4Compressor;
import org.apache.cassandra.io.compress.SnappyCompressor;
import org.apache.cassandra.io.sstable.format.CompressionInfoComponent;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Distributed tests for pluggable compression providers configured via
 * the {@code compressor_providers} option in cassandra.yaml.
 * <p>
 * Covers (CASSANDRA-20975):
 * - default providers are registered when no plugin is configured;
 * - a custom healthy provider is wired into the registry and used by table compression;
 * - an unhealthy provider falls back to the default when fallback is enabled;
 * - an unhealthy provider fails node startup when fallback is disabled.
 */
public class CompressorProviderTest extends TestBaseImpl
{
    @Test
    public void testDefaultProvidersRegisteredWhenNoConfig() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(1).start()))
        {
            cluster.get(1).runOnInstance(() -> {
                for (CompressorRegistry.CompressorType type : CompressorRegistry.CompressorType.values())
                {
                    AbstractCompressionProvider provider = CompressorRegistry.instance.getProvider(type.compressorClass());
                    assertNotNull("Provider missing for " + type, provider);
                    assertSame("Built-in compressor " + type + " must use the default provider when none is configured",
                               DefaultCompressionProvider.class, provider.getClass());
                }
            });
        }
    }

    @Test
    public void testCustomProviderRegisteredAndUsedForTableCompression() throws Throwable
    {
        Map<String, Object> snappyProviderOptions = new HashMap<>();
        snappyProviderOptions.put("class_name", HealthyTestProvider.class.getName());
        snappyProviderOptions.put("parameters", Map.of(AbstractCompressionProvider.FAIL_ON_MISSING_PROVIDER, Boolean.TRUE.toString()));

        Map<String, Object> providers = new HashMap<>();
        providers.put(SnappyCompressor.class.getSimpleName(), snappyProviderOptions);

        try (Cluster cluster = init(Cluster.build(1)
                                           .withConfig(config -> config.set("compressor_providers", providers))
                                           .start()))
        {
            // The registry must hold our custom provider for SnappyCompressor and the default
            // for every other built-in type.
            cluster.get(1).runOnInstance(() -> {
                AbstractCompressionProvider snappy = CompressorRegistry.instance.getProvider(SnappyCompressor.class);
                assertNotNull(snappy);
                assertEquals(HealthyTestProvider.class.getName(), snappy.getClass().getName());

                for (CompressorRegistry.CompressorType type : CompressorRegistry.CompressorType.values())
                {
                    if (type.compressorClass() == SnappyCompressor.class)
                        continue;
                    AbstractCompressionProvider provider = CompressorRegistry.instance.getProvider(type.compressorClass());
                    assertSame(DefaultCompressionProvider.class, provider.getClass());
                }
            });

            // Reset the counter so the assertion below only sees calls produced by this test's DDL/writes.
            cluster.get(1).runOnInstance(() -> HealthyTestProvider.CREATE_CALLS.set(0));

            // End-to-end: create a table that uses Snappy compression. The compressor must be sourced
            // through HealthyTestProvider; writes and reads must succeed because the provider
            // delegates compressor creation to the default provider.
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".plugin_snappy (k int PRIMARY KEY, v text) " +
                                 "WITH compression = {'class': 'SnappyCompressor', 'chunk_length_in_kb': '4'}");

            for (int i = 0; i < 100; i++)
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".plugin_snappy (k, v) VALUES (?, ?)",
                                               ConsistencyLevel.ONE, i, "value-" + i);

            cluster.get(1).flush(KEYSPACE);

            Object[][] rows = cluster.coordinator(1).execute("SELECT count(*) FROM " + KEYSPACE + ".plugin_snappy",
                                                             ConsistencyLevel.ONE);
            assertEquals(100L, rows[0][0]);

            int creates = cluster.get(1).callOnInstance((SerializableCallable<Integer>) () -> HealthyTestProvider.CREATE_CALLS.get());
            assertTrue("HealthyTestProvider.createCompressor should have been invoked for table compression, was " + creates,
                       creates > 0);

            int createsSnappy = cluster.get(1).callOnInstance((SerializableCallable<Integer>) () -> HealthyTestProvider.CREATE_SNAPPY_CALLS.get());
            assertTrue("HealthyTestProvider.createCompressor should have been invoked for table compression, was " + createsSnappy,
                       createsSnappy > 0);

            // The schema must record the *concrete* compressor class, not the provider — this is what
            // peers and future restarts use to rebuild the compression chain.
            Object[][] schemaRows = cluster.coordinator(1).execute(
            "SELECT compression FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?",
            ConsistencyLevel.ONE, KEYSPACE, "plugin_snappy");
            @SuppressWarnings("unchecked")
            Map<String, String> compressionOptions = (Map<String, String>) schemaRows[0][0];
            assertEquals("system_schema.tables.compression must record the concrete compressor class, not the provider",
                         SnappyCompressor.class.getName(), compressionOptions.get("class"));

            // Reconstruct CompressionMetadata from the flushed SSTable on disk and check the
            // rebuilt compressor's serializedAs() — this is the class name that was actually
            // written into *-CompressionInfo.db, and the one a peer without the plugin would
            // read. getClass() on the rebuilt instance would just yield MySnappyCompressor again
            // because the plugin replays its substitution during CompressionMetadata.open.
            String onDiskSerializedAs = cluster.get(1).callOnInstance((SerializableCallable<String>) () -> {
                ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore("plugin_snappy");
                SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
                try (CompressionMetadata metadata = CompressionInfoComponent.loadIfExists(sstable.descriptor, null))
                {
                    Assert.assertNotNull(metadata);
                    return metadata.parameters.getSstableCompressor().serializedAs().getName();
                }
            });
            assertEquals("Rebuilt CompressionMetadata.serializedAs() must yield the underlying compressor - " +
                         "what the plugin advertises for portability - not the wrapper",
                         SnappyCompressor.class.getName(), onDiskSerializedAs);
        }
    }

    @Test
    public void testCompressWithCustomProviderAndDecompressWithDefault() throws Throwable
    {
        //This test will simulate a scenario where data is compressed with a custom provider and then read on a node which does not have the provider configured,
        // In the initial phase the test does write/read checks like testCustomProviderRegisteredAndUsedForTableCompression then it brings down the cluster,
        // restarts with DefaultCompressionProvider for SnappyCompressor, and checks that the data can still be read.
        Map<String, Object> snappyProviderOptions = new HashMap<>();
        snappyProviderOptions.put("class_name", HealthyTestProvider.class.getName());
        snappyProviderOptions.put("parameters", Map.of(AbstractCompressionProvider.FAIL_ON_MISSING_PROVIDER, Boolean.TRUE.toString()));

        Map<String, Object> providers = new HashMap<>();
        providers.put(SnappyCompressor.class.getSimpleName(), snappyProviderOptions);

        try (Cluster cluster = init(Cluster.build(1)
                                           .withConfig(config -> config.set("compressor_providers", providers))
                                           .start()))
        {
            // Mae sure registry holds the custom provider for SnappyCompressor
            cluster.get(1).runOnInstance(() -> {
                AbstractCompressionProvider snappy = CompressorRegistry.instance.getProvider(SnappyCompressor.class);
                assertNotNull(snappy);
                assertEquals(HealthyTestProvider.class.getName(), snappy.getClass().getName());
            });

            // Reset the counter so the assertion below only sees calls produced by this test's DDL/writes.
            cluster.get(1).runOnInstance(() -> HealthyTestProvider.CREATE_CALLS.set(0));

            // End-to-end: create a table that uses Snappy compression
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".plugin_snappy (k int PRIMARY KEY, v text) " +
                                 "WITH compression = {'class': 'SnappyCompressor', 'chunk_length_in_kb': '4'}");

            for (int i = 0; i < 100; i++)
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".plugin_snappy (k, v) VALUES (?, ?)",
                                               ConsistencyLevel.ONE, i, "value-" + i);

            cluster.get(1).flush(KEYSPACE);

            Object[][] rows = cluster.coordinator(1).execute("SELECT count(*) FROM " + KEYSPACE + ".plugin_snappy",
                                                             ConsistencyLevel.ONE);
            assertEquals(100L, rows[0][0]);

            int creates = cluster.get(1).callOnInstance((SerializableCallable<Integer>) () -> HealthyTestProvider.CREATE_CALLS.get());
            assertTrue("HealthyTestProvider.createCompressor should have been invoked for table compression, was " + creates,
                       creates > 0);

            int createsSnappy = cluster.get(1).callOnInstance((SerializableCallable<Integer>) () -> HealthyTestProvider.CREATE_SNAPPY_CALLS.get());
            assertTrue("HealthyTestProvider.createCompressor should have been invoked for table compression, was " + createsSnappy,
                       createsSnappy > 0);

            // Ensure schema records the *concrete* compressor class, not the provider
            Object[][] schemaRows = cluster.coordinator(1).execute(
            "SELECT compression FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?",
            ConsistencyLevel.ONE, KEYSPACE, "plugin_snappy");
            @SuppressWarnings("unchecked")
            Map<String, String> compressionOptions = (Map<String, String>) schemaRows[0][0];
            assertEquals("system_schema.tables.compression must record the concrete compressor class, not the provider",
                         SnappyCompressor.class.getName(), compressionOptions.get("class"));

            // Reconstruct CompressionMetadata from the flushed SSTable on disk and check the
            // rebuilt compressor's serializedAs()
            String onDiskSerializedAs = cluster.get(1).callOnInstance((SerializableCallable<String>) () -> {
                ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore("plugin_snappy");
                SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
                try (CompressionMetadata metadata = CompressionInfoComponent.loadIfExists(sstable.descriptor, null))
                {
                    Assert.assertNotNull(metadata);
                    return metadata.parameters.getSstableCompressor().serializedAs().getName();
                }
            });
            assertEquals("Rebuilt CompressionMetadata.serializedAs() must yield the underlying compressor - " +
                         "what the plugin advertises for portability - not the wrapper",
                         SnappyCompressor.class.getName(), onDiskSerializedAs);

            // Simulate a node which does not have plugin configured
            // Shut down the cluster, change provider class name to default, and restart. It should be able to use the Default provider for SnappyCompressor,
            // and be able to read the existing data.
            cluster.get(1).shutdown().get();
            snappyProviderOptions = new HashMap<>();
            snappyProviderOptions.put("class_name", DefaultCompressionProvider.class.getName());
            snappyProviderOptions.put("parameters", Map.of(AbstractCompressionProvider.FAIL_ON_MISSING_PROVIDER, Boolean.TRUE.toString()));
            providers.clear();
            providers.put(SnappyCompressor.class.getSimpleName(), snappyProviderOptions);
            cluster.get(1).config().set("compressor_providers", providers);
            cluster.get(1).startup();
            cluster.get(1).runOnInstance(() -> {
                AbstractCompressionProvider snappy = CompressorRegistry.instance.getProvider(SnappyCompressor.class);
                assertNotNull(snappy);
                assertEquals(DefaultCompressionProvider.class.getName(), snappy.getClass().getName());
            });

            //The node should be able to decompress the SSTables
            // that were written by the plugin node, because serializedAs() ensured the
            // on-disk CompressionInfo.db recorded SnappyCompressor (not MySnappyCompressor).
            Object[][] rowsOnNewNode = cluster.coordinator(1).execute(
            "SELECT count(*) FROM " + KEYSPACE + ".plugin_snappy", ConsistencyLevel.ONE);
            assertEquals("The new node which does not have the plugin must read all rows written by the plugin node",
                         100L, rowsOnNewNode[0][0]);

        }
    }

    @Test
    public void testCompressOn2NodesWithAndWithoutCustomProvider() throws Throwable
    {
        // This test will simulate a scenario with a cluster containing 2 nodes, one with custom provider configured and another with default provider.
        // A table is created with Snappy compression on the first node with a custom provider, writes data and flushes to disk. Data is then read from both nodes
        // to verify that both can read the data regardless of the provider they have configured.
        Map<String, Object> snappyProviderOptionsCustom = new HashMap<>();
        snappyProviderOptionsCustom.put("class_name", HealthyTestProvider.class.getName());
        snappyProviderOptionsCustom.put("parameters", Map.of(AbstractCompressionProvider.FAIL_ON_MISSING_PROVIDER, Boolean.TRUE.toString()));

        Map<String, Object> snappyProviderOptionsDefault = new HashMap<>();
        snappyProviderOptionsDefault.put("class_name", DefaultCompressionProvider.class.getName());
        snappyProviderOptionsDefault.put("parameters", Map.of(AbstractCompressionProvider.FAIL_ON_MISSING_PROVIDER, Boolean.TRUE.toString()));

        Map<String, Object> firstNodeProvider = new HashMap<>();
        firstNodeProvider.put(SnappyCompressor.class.getSimpleName(), snappyProviderOptionsCustom);
        Map<String, Object> secondNodeProvider = new HashMap<>();
        secondNodeProvider.put(SnappyCompressor.class.getSimpleName(), snappyProviderOptionsDefault);

        try (Cluster cluster = init(Cluster.build(2)
                                           .withConfig(config -> {
                                               if (config.num() == 1)
                                                   config.set("compressor_providers", firstNodeProvider);   // custom HealthyTestProvider
                                               else
                                                   config.set("compressor_providers", secondNodeProvider);  // DefaultCompressionProvider
                                               config.set("accord.enabled", false); // disable accord to avoid schema disagreement during startup due to different compressor providers
                                           })
                                           .start()))
        {
            // First node must use HealthyTestProvider for SnappyCompressor,
            // Second node must use DefaultCompressionProvider for SnappyCompressor
            cluster.get(1).runOnInstance(() -> {
                AbstractCompressionProvider snappy = CompressorRegistry.instance.getProvider(SnappyCompressor.class);
                assertNotNull(snappy);
                assertEquals(HealthyTestProvider.class.getName(), snappy.getClass().getName());
            });
            cluster.get(2).runOnInstance(() -> {
                AbstractCompressionProvider snappy = CompressorRegistry.instance.getProvider(SnappyCompressor.class);
                assertNotNull(snappy);
                assertEquals(DefaultCompressionProvider.class.getName(), snappy.getClass().getName());
            });


            // Reset the counter so the assertion below only sees calls produced by this test's DDL/writes.
            cluster.get(1).runOnInstance(() -> HealthyTestProvider.CREATE_CALLS.set(0));

            // End-to-end: create a table that uses Snappy compression.
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".plugin_snappy (k int PRIMARY KEY, v text) " +
                                 "WITH compression = {'class': 'SnappyCompressor', 'chunk_length_in_kb': '4'}");

            for (int i = 0; i < 100; i++)
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".plugin_snappy (k, v) VALUES (?, ?)",
                                               ConsistencyLevel.ALL, i, "value-" + i);

            cluster.get(1).flush(KEYSPACE);
            cluster.get(2).flush(KEYSPACE);

            // Read from node 1
            Object[][] rowsFromNode1 = cluster.coordinator(1).execute("SELECT count(*) FROM " + KEYSPACE + ".plugin_snappy",
                                                                      ConsistencyLevel.ONE);
            assertEquals(100L, rowsFromNode1[0][0]);

            // Read from node 2
            Object[][] rowsFromNode2 = cluster.coordinator(2).execute("SELECT count(*) FROM " + KEYSPACE + ".plugin_snappy",
                                                                      ConsistencyLevel.ONE);
            assertEquals(100L, rowsFromNode2[0][0]);

            int creates = cluster.get(1).callOnInstance((SerializableCallable<Integer>) () -> HealthyTestProvider.CREATE_CALLS.get());
            assertTrue("HealthyTestProvider.createCompressor should have been invoked for table compression, was " + creates,
                       creates > 0);

            int createsSnappy = cluster.get(1).callOnInstance((SerializableCallable<Integer>) () -> HealthyTestProvider.CREATE_SNAPPY_CALLS.get());
            assertTrue("HealthyTestProvider.createCompressor should have been invoked for table compression, was " + createsSnappy,
                       createsSnappy > 0);

            // The schema must record the *concrete* compressor class, not the provider — this is what
            // peers and future restarts use to rebuild the compression chain.
            Object[][] schemaRows = cluster.coordinator(1).execute(
            "SELECT compression FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?",
            ConsistencyLevel.ALL, KEYSPACE, "plugin_snappy");
            @SuppressWarnings("unchecked")
            Map<String, String> compressionOptions = (Map<String, String>) schemaRows[0][0];
            assertEquals("system_schema.tables.compression must record the concrete compressor class, not the provider",
                         SnappyCompressor.class.getName(), compressionOptions.get("class"));

            // Reconstruct CompressionMetadata from the flushed SSTable on disk and check the
            // rebuilt compressor's serializedAs()
            String onDiskSerializedAsNode1 = cluster.get(1).callOnInstance((SerializableCallable<String>) () -> {
                ColumnFamilyStore cfs = Keyspace.open(KEYSPACE)
                                                .getColumnFamilyStore("plugin_snappy");
                SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
                try (CompressionMetadata metadata = CompressionInfoComponent.loadIfExists(sstable.descriptor, null))
                {
                    Assert.assertNotNull(metadata);
                    return metadata.parameters.getSstableCompressor().serializedAs().getName();
                }
            });
            assertEquals("Rebuilt CompressionMetadata.serializedAs() must yield the underlying compressor - " +
                         "what the plugin advertises for portability - not the wrapper",
                         SnappyCompressor.class.getName(), onDiskSerializedAsNode1);

            // Node 2 (DefaultCompressionProvider): also verify the on-disk SSTable is readable and
            // records SnappyCompressor — confirming it can decompress without the plugin.
            String onDiskSerializedAsNode2 = cluster.get(2).callOnInstance((SerializableCallable<String>) () -> {
                ColumnFamilyStore cfs = Keyspace.open(KEYSPACE)
                                                .getColumnFamilyStore("plugin_snappy");
                SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
                try (CompressionMetadata metadata = CompressionInfoComponent.loadIfExists(sstable.descriptor, null))
                {
                    Assert.assertNotNull(metadata);
                    return metadata.parameters.getSstableCompressor().serializedAs().getName();
                }
            });
            assertEquals("Node 2 (DefaultCompressionProvider): on-disk serializedAs() must also yield the concrete compressor class",
                         SnappyCompressor.class.getName(), onDiskSerializedAsNode2);
        }

    }

    @Test
    public void testUnhealthyProviderWithFallbackBootsWithDefault() throws Throwable
    {
        Map<String, Object> lz4ProviderOptions = new HashMap<>();
        lz4ProviderOptions.put("class_name", UnhealthyTestProvider.class.getName());
        lz4ProviderOptions.put("parameters", Map.of(AbstractCompressionProvider.FAIL_ON_MISSING_PROVIDER, Boolean.FALSE.toString()));

        Map<String, Object> providers = new HashMap<>();
        providers.put(LZ4Compressor.class.getSimpleName(), lz4ProviderOptions);

        try (Cluster cluster = init(Cluster.build(1)
                                           .withConfig(config -> config.set("compressor_providers", providers))
                                           .start()))
        {
            cluster.get(1).runOnInstance(() -> {
                AbstractCompressionProvider lz4Provider = CompressorRegistry.instance.getProvider(LZ4Compressor.class);
                assertNotNull(lz4Provider);
                assertSame("Unhealthy provider with fallback=true must resolve to the default",
                           DefaultCompressionProvider.class,
                           lz4Provider.getClass());
            });

            // Sanity check: table-level LZ4 compression still works because the fallback default is registered.
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".fallback_lz4 (k int PRIMARY KEY, v text) " +
                                 "WITH compression = {'class': 'LZ4Compressor'}");

            cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".fallback_lz4 (k, v) VALUES (1, 'ok')",
                                           ConsistencyLevel.ONE);
            cluster.get(1).flush(KEYSPACE);

            Object[][] rows = cluster.coordinator(1).execute("SELECT v FROM " + KEYSPACE + ".fallback_lz4 WHERE k = 1",
                                                             ConsistencyLevel.ONE);
            assertEquals("ok", rows[0][0]);
        }
    }

    @Test
    public void testUnhealthyProviderWithoutFallbackFailsStartup() throws Throwable
    {
        Map<String, Object> lz4ProviderOptions = new HashMap<>();
        lz4ProviderOptions.put("class_name", UnhealthyTestProvider.class.getName());
        lz4ProviderOptions.put("parameters", Map.of(AbstractCompressionProvider.FAIL_ON_MISSING_PROVIDER, Boolean.TRUE.toString()));

        Map<String, Object> providers = new HashMap<>();
        providers.put(LZ4Compressor.class.getSimpleName(), lz4ProviderOptions);

        try (Cluster cluster = Cluster.build(1)
                                      .withConfig(config -> config.set("compressor_providers", providers))
                                      .createWithoutStarting())
        {
            try
            {
                cluster.get(1).startup();
                fail("Startup should have failed because the configured compressor provider is unhealthy and fallback is disabled");
            }
            catch (Throwable t)
            {
                Throwable cause = t;
                String expected = "Failed to initialize compression provider";
                while (cause != null && (cause.getMessage() == null || !cause.getMessage().contains(expected)))
                    cause = cause.getCause();
                assertNotNull("Expected ConfigurationException containing '" + expected + "' in the cause chain, got: " + t, cause);
            }
        }
    }

    @Test
    public void testUnknownCompressorKeyFailsStartup() throws Throwable
    {
        // 'no_such_compressor' matches no built-in compressor (neither FQCN nor simple name nor
        // abbreviation), so registerProviders must reject it at apply-time and the node must
        // refuse to start with a ConfigurationException.
        Map<String, Object> bogusProviderOptions = new HashMap<>();
        bogusProviderOptions.put("class_name", HealthyTestProvider.class.getName());
        bogusProviderOptions.put("parameters", Map.of(AbstractCompressionProvider.FAIL_ON_MISSING_PROVIDER, Boolean.TRUE.toString()));

        Map<String, Object> providers = new HashMap<>();
        providers.put("no_such_compressor", bogusProviderOptions);

        try (Cluster cluster = Cluster.build(1)
                                      .withConfig(config -> config.set("compressor_providers", providers))
                                      .createWithoutStarting())
        {
            try
            {
                cluster.get(1).startup();
                fail("Startup should have failed because compressor_providers contains a key that maps to no known compressor");
            }
            catch (Throwable t)
            {
                Throwable configException = t;
                String expectedClass = "org.apache.cassandra.exceptions.ConfigurationException";
                String expectedMessage = "Unknown compressor key 'no_such_compressor'";
                while (configException != null
                       && (!expectedClass.equals(configException.getClass().getName())
                           || configException.getMessage() == null
                           || !configException.getMessage().contains(expectedMessage)))
                    configException = configException.getCause();
                assertNotNull("Expected ConfigurationException with message containing '" + expectedMessage
                              + "' in the cause chain, got: " + t, configException);
            }
        }
    }

    /**
     * A custom provider that reports healthy and delegates compressor creation to the
     * built-in default provider. Used to prove the registry actually invokes the plugin
     * during table compression rather than silently using the default.
     * <p>
     * Must be public with a no-arg constructor so {@code FBUtilities.newCompressionProvider}
     * can instantiate it via reflection.
     */
    public static class HealthyTestProvider extends AbstractCompressionProvider
    {
        // Statics live in the InstanceClassLoader where the provider is loaded. Always read and
        // mutate these via runOnInstance / callOnInstance — touching them directly from the test
        // thread reads a separate copy in the test classloader and silently returns zero.
        public static final AtomicInteger CREATE_CALLS = new AtomicInteger();
        public static final AtomicInteger CREATE_SNAPPY_CALLS = new AtomicInteger();

        @Override
        public boolean isHealthy()
        {
            return true;
        }

        @Override
        public ICompressor createCompressor(Class<?> compressorClass, Map<String, String> options)
        {
            CREATE_CALLS.incrementAndGet();

            // inject here our custom compressor, mimmicking pluggable providers
            if (compressorClass == SnappyCompressor.class)
            {
                CREATE_SNAPPY_CALLS.incrementAndGet();
                return new MySnappyCompressor();
            }

            return CompressorRegistry.DEFAULT_COMPRESSION_PROVIDER.createCompressor(compressorClass, options);
        }
    }

    public static class MySnappyCompressor extends SnappyCompressor
    {
        @Override
        public Class<? extends ICompressor> serializedAs()
        {
            // Tell Cassandra to record SnappyCompressor in schema/SSTable metadata so that the
            // schema is portable to peers and restarts without this plugin.
            return SnappyCompressor.class;
        }
    }

    public static class UnhealthyTestProvider extends AbstractCompressionProvider
    {
        @Override
        public boolean isHealthy()
        {
            return false;
        }

        @Override
        public ICompressor createCompressor(Class<?> compressorClass, Map<String, String> options)
        {
            throw new IllegalStateException("unhealthy provider must not be asked to create compressors");
        }
    }
}
