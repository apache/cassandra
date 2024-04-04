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

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.io.compress.DeflateCompressor;
import org.apache.cassandra.io.compress.LZ4Compressor;
import org.apache.cassandra.io.compress.NoopCompressor;
import org.apache.cassandra.io.compress.SnappyCompressor;
import org.apache.cassandra.io.compress.ZstdCompressor;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.service.StorageService;

import static com.google.common.base.Charsets.UTF_8;
import static java.lang.String.format;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.test.ExecUtil.rethrow;
import static org.apache.cassandra.schema.SchemaConstants.LOCAL_SYSTEM_KEYSPACE_NAMES;
import static org.apache.cassandra.schema.SchemaConstants.REPLICATED_SYSTEM_KEYSPACE_NAMES;
import static org.apache.commons.io.FileUtils.readFileToString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class SSTableCompressionTest
{
    static Cluster CLUSTER;
    static String NODES;
    static int NATIVE_PORT;
    static int STORAGE_PORT;
    static int SSL_STORAGE_PORT;

    private static final String FAST_PARAM = "WITH compression = {'class': 'SnappyCompressor'}";
    private static final String SLOW_PARAM = "WITH compression = {'class': 'DeflateCompressor'}";
    private static final String DEFAULT_PARAM = "";

    private static final String KEYSPACE = "sstable_compression_test";

    private static final Consumer<IInstanceConfig> DEFAULT_CONFIG = c -> {
        c.with(NATIVE_PROTOCOL, NETWORK, GOSSIP); // need gossip to get hostid for Java driver
        c.set("flush_compression", "fast"); // this is default
        Config.SSTableConfig config = new Config.SSTableConfig();
        config.default_compression = new ParameterizedClass("lz4");
        c.set("sstable", config);
    };

    private static final Consumer<IInstanceConfig> FAST_CONFIG = c -> {
        c.with(NATIVE_PROTOCOL, NETWORK, GOSSIP); // need gossip to get hostid for Java driver
        c.set("flush_compression", "fast"); // this is default
        Config.SSTableConfig config = new Config.SSTableConfig();
        config.default_compression = new ParameterizedClass("snappy");
        c.set("sstable", config);
    };

    private static final Consumer<IInstanceConfig> SLOW_CONFIG = c -> {
        c.with(NATIVE_PROTOCOL, NETWORK, GOSSIP); // need gossip to get hostid for Java driver
        c.set("flush_compression", "fast"); // this is default
        Config.SSTableConfig config = new Config.SSTableConfig();
        config.default_compression = new ParameterizedClass("deflate");
        c.set("sstable", config);
    };

    private static final Consumer<IInstanceConfig> ZSTD_CONFIG = c -> {
        c.with(NATIVE_PROTOCOL, NETWORK, GOSSIP); // need gossip to get hostid for Java driver
        c.set("flush_compression", "fast"); // this is default
        Config.SSTableConfig config = new Config.SSTableConfig();
        config.default_compression = new ParameterizedClass("zstd");
        c.set("sstable", config);
    };

    public static Path setupCluster(Consumer<IInstanceConfig> config, Path root) throws IOException
    {
        Cluster.Builder builder = Cluster.build().withNodes(1).withConfig(config);

        if (root != null)
            builder.withRoot(root);

        CLUSTER = builder.start();
        NODES = CLUSTER.get(1).config().broadcastAddress().getHostString();
        NATIVE_PORT = CLUSTER.get(1).callOnInstance(DatabaseDescriptor::getNativeTransportPort);
        STORAGE_PORT = CLUSTER.get(1).callOnInstance(DatabaseDescriptor::getStoragePort);
        SSL_STORAGE_PORT = CLUSTER.get(1).callOnInstance(DatabaseDescriptor::getSSLStoragePort);

        return builder.getRootPath();
    }

    public static void tearDownCluster()
    {
        if (CLUSTER != null)
            CLUSTER.close();
    }

    @Test
    public void testCreation() throws IOException
    {
        // create cluster with lz4 compression
        try
        {
            setupCluster(DEFAULT_CONFIG, null);
            CLUSTER.schemaChange(format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}", KEYSPACE));
            testDisabled();
            testDeflate();
            testLZ4();
            testNoop();
            testSnappy();
            testZstd();
        }
        finally
        {
            tearDownCluster();
        }
    }

    /**
     * This tests shows that defining a table with a different compressor works and that changing
     * the default compressor between starts does not change the compressors associated with the
     * saved (snapshotted) systems.
     */
    @Test
    public void configChangeIsolation() throws Throwable
    {
        try
        {
            Path root = setupCluster(DEFAULT_CONFIG, null);
            CLUSTER.schemaChange(format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}", KEYSPACE));
            createSSTable("default", DEFAULT_PARAM);
            createSSTable("different_compress", FAST_PARAM);

            populateTable("default");
            populateTable("different_compress");
            flushTables();

            Map<String, String> parameters = getCompressionParameters(CLUSTER.get(1), "default");
            assertEquals(LZ4Compressor.class.getName(), parameters.get("class"));
            parameters = getCompressionParameters(CLUSTER.get(1), "different_compress");
            assertEquals(SnappyCompressor.class.getName(), parameters.get("class"));

            // shutdown but do not delete files.
            CLUSTER.close(false);

            // restart with Deflate compression definition
            setupCluster(SLOW_CONFIG, root);

            // create and populate a new table
            CLUSTER.schemaChange(format("CREATE TABLE IF NOT EXISTS %s.new_table (pk int, val text, PRIMARY KEY (pk))", KEYSPACE));
            flushTables();

            parameters = getCompressionParameters(CLUSTER.get(1), "default");
            assertEquals(LZ4Compressor.class.getName(), parameters.get("class"));

            parameters = getCompressionParameters(CLUSTER.get(1), "different_compress");
            assertEquals(SnappyCompressor.class.getName(), parameters.get("class"));

            parameters = getCompressionParameters(CLUSTER.get(1), "new_table");
            assertEquals(DeflateCompressor.class.getName(), parameters.get("class"));
        }
        finally
        {
            tearDownCluster();
        }
    }

    /**
     * This tests shows that defining a table with a different compressor works and that changing
     * the default compressor between starts does not change  the compressors associated with the
     * saved (snapshotted) systems.
     */
    @Test
    public void compressionNotChangedInSnapshotIO() throws Throwable
    {
        try
        {
            Path root = setupCluster(DEFAULT_CONFIG, null);
            CLUSTER.schemaChange(format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}", KEYSPACE));
            createSSTable("different_compress", "WITH compression = {'class': 'ZstdCompressor'}");

            populateTable("different_compress");
            flushTables();

            Map<String, String> parameters = getCompressionParameters(CLUSTER.get(1), "different_compress");
            assertEquals(ZstdCompressor.class.getName(), parameters.get("class"));

            Set<String> snapshot1 = snapshot(CLUSTER.get(1), "different_compress", "test");

            // shutdown but do not delete files.
            CLUSTER.close(false);

            // restart with Deflate compression definition
            setupCluster(SLOW_CONFIG, root);

            restore(CLUSTER.get(1), snapshot1, "different_compress");

            assertSnapshotCompression(snapshot(CLUSTER.get(1), "different_compress", "backup1"), ImmutableSet.of("LZ4Compressor"));
            parameters = getCompressionParameters(CLUSTER.get(1), "different_compress");
            assertEquals(ZstdCompressor.class.getName(), parameters.get("class"));
        }
        finally
        {
            tearDownCluster();
        }
    }

    @Test
    public void testEnableDisableCompression() throws Throwable
    {
        try
        {
            setupCluster(ZSTD_CONFIG, null);

            CLUSTER.schemaChange(format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}", KEYSPACE));
            createSSTable("sometable", DEFAULT_PARAM);

            assertEquals(ZstdCompressor.class.getName(), getCompressionParameters(CLUSTER.get(1), "sometable").get("class"));

            // if we transition from false to true, it will use the default compressor in sstable_compression
            CLUSTER.schemaChange(format("ALTER TABLE %s.%s WITH compression = {'enabled': false}", KEYSPACE, "sometable"));
            CLUSTER.schemaChange(format("ALTER TABLE %s.%s WITH compression = {'enabled': true}", KEYSPACE, "sometable"));

            assertEquals(ZstdCompressor.class.getName(), getCompressionParameters(CLUSTER.get(1), "sometable").get("class"));
        }
        finally
        {
            tearDownCluster();
        }
    }

    @Test
    public void testDefaultCompressionDoesNotApplyToSystemKeyspaces() throws Throwable
    {
        try
        {
            setupCluster(ZSTD_CONFIG, null);

            for (Map.Entry<String, Map<String, String>> entry : getCompressionParametersForSystemKeyspaces(CLUSTER.get(1)).entrySet())
                assertNotEquals(format("compression for table %s should not be changed by sstable_compression for system keyspaces!", entry.getKey()),
                                ZstdCompressor.class.getName(), entry.getValue().get("class"));
        }
        finally
        {
            tearDownCluster();
        }
    }

    private void testCreate(String table, String tableArgs, Map<String, String> expected)
    {
        createSSTable(table, tableArgs);
        Map<String, String> parameters = getCompressionParameters(CLUSTER.get(1), table);
        assertEquals("Wrong size for table: " + table, expected.size(), parameters.size());
        for (Map.Entry<String, String> e : expected.entrySet())
            assertEquals("Missing value for table: " + table, e.getValue(), parameters.get(e.getKey()));
    }

    private Map<String, String> createDefaultMap(Class<?> clazz)
    {
        return new HashMap<String, String>()
        {{
            put("class", clazz.getName());
            put("chunk_length_in_kb", "16");
        }};
    }

    private void testLZ4()
    {
        Map<String, String> expected = createDefaultMap(LZ4Compressor.class);
        testCreate("lz4", "", expected);
        testCreate("lz4_arg", "WITH compression = {'class': 'LZ4Compressor'}", expected);
        expected.put("chunk_length_in_kb", "8");
        testCreate("lz4_chunk", "WITH compression = {'chunk_length_in_kb' : '8'}", expected);
        expected.put("chunk_length_in_kb", "16");
        expected.put("lz4_high_compressor_level", "3");
        testCreate("lz4_arg_high", "WITH compression = {'class': 'LZ4Compressor', 'lz4_high_compressor_level':'3' }", expected);
        expected.remove("lz4_high_compressor_level");
        expected.put("lz4_compressor_type", "fast");
        testCreate("lz4_arg_type", "WITH compression = {'class': 'LZ4Compressor', 'lz4_compressor_type':'fast' }", expected);
    }

    private void testDisabled()
    {
        Map<String, String> expected = new HashMap<>();
        expected.put("enabled", "false");
        testCreate("default_disabled", "WITH compression = { 'enabled':'false'}", expected);
    }

    private void testDeflate()
    {
        Map<String, String> expected = createDefaultMap(DeflateCompressor.class);
        testCreate("dflt", SLOW_PARAM, expected);
        expected.put("chunk_length_in_kb", "8");
        testCreate("dflt_chunk", "WITH compression = {'class':'DeflateCompressor', 'chunk_length_in_kb' : '8'}", expected);
    }

    private void testNoop()
    {
        Map<String, String> expected = createDefaultMap(NoopCompressor.class);
        testCreate("noop", "WITH compression = {'class': 'NoopCompressor'}", expected);
        expected.put("chunk_length_in_kb", "8");
        testCreate("noop_chunk", "WITH compression = {'class':'NoopCompressor', 'chunk_length_in_kb' : '8'}", expected);
    }

    private void testSnappy()
    {
        Map<String, String> expected = createDefaultMap(SnappyCompressor.class);
        testCreate("snappy", FAST_PARAM, expected);
        expected.put("chunk_length_in_kb", "8");
        testCreate("snappy_chunk", "WITH compression = {'class':'SnappyCompressor', 'chunk_length_in_kb' : '8'}", expected);
    }

    private void testZstd()
    {
        Map<String, String> expected = createDefaultMap(ZstdCompressor.class);
        testCreate("zstd", "WITH compression = {'class': 'ZstdCompressor'}", expected);
        expected.put("chunk_length_in_kb", "8");
        testCreate("zstd_chunk", "WITH compression = {'class':'ZstdCompressor', 'chunk_length_in_kb' : '8'}", expected);
        expected.put("chunk_length_in_kb", "16");
        expected.put("compression_level", "5");
        testCreate("zstd_compress", "WITH compression = {'class': 'ZstdCompressor', 'compression_level':'5'}", expected);
    }

    private void flushTables()
    {
        CLUSTER.get(1).runOnInstance(rethrow(() -> StorageService.instance.forceKeyspaceFlush(KEYSPACE, ColumnFamilyStore.FlushReason.UNIT_TESTS)));
    }

    private void assertSnapshotCompression(Set<String> paths, Set<String> types) throws IOException
    {
        List<File> compressionInfos = paths.stream()
                                           .map(p -> {
                                               try
                                               {
                                                   return new File(p).list((d, n) -> n.endsWith("CompressionInfo.db"));
                                               }
                                               catch (Exception e)
                                               {
                                                   return new File[0];
                                               }
                                           })
                                           .filter(f -> f.length > 0)
                                           .map(f -> f[0])
                                           .collect(Collectors.toList());

        List<String> typesToExpect = new ArrayList<>(types);

        for (File compressionInfo : compressionInfos)
        {
            if (typesToExpect.isEmpty())
                return;

            String content = readFileToString(compressionInfo.toJavaIOFile(), UTF_8);
            String foundType = null;
            for (String expectedType : types)
            {
                if (content.contains(expectedType))
                {
                    foundType = expectedType;
                    break;
                }
            }

            if (foundType != null)
                typesToExpect.remove(foundType);
        }

        assertTrue(typesToExpect.isEmpty());
    }

    private void restore(IInvokableInstance instance, Set<String> dirs, String targetTableName)
    {
        List<String> failedImports = instance.callOnInstance(() -> ColumnFamilyStore.getIfExists(KEYSPACE, targetTableName)
                                                                                    .importNewSSTables(dirs,
                                                                                                       false,
                                                                                                       false,
                                                                                                       true,
                                                                                                       true,
                                                                                                       true,
                                                                                                       true,
                                                                                                       true));
        assertThat(failedImports).isEmpty();
    }

    private Map<String, Map<String, String>> getCompressionParametersForSystemKeyspaces(IInvokableInstance instance)
    {
        try
        {
            return instance.callOnInstance(() -> Keyspace.allExisting()
                                                         .filter(ks -> LOCAL_SYSTEM_KEYSPACE_NAMES.contains(ks.getName()) || REPLICATED_SYSTEM_KEYSPACE_NAMES.contains(ks.getName()))
                                                         .map(Keyspace::getColumnFamilyStores)
                                                         .flatMap(Collection::stream)
                                                         .collect(Collectors.toMap(e -> e.getKeyspaceName() + '.' + e.getTableName(),
                                                                                   ColumnFamilyStore::getCompressionParameters)));
        }
        catch (Exception e)
        {
            throw new RuntimeException("error getting parameters for all tables", e);
        }
    }

    private Map<String, String> getCompressionParameters(IInvokableInstance instance, String tableName)
    {
        try
        {
            return instance.callOnInstance(() -> ColumnFamilyStore.getIfExists(KEYSPACE, tableName).getCompressionParameters());
        }
        catch (Exception e)
        {
            throw new RuntimeException("error getting parameters for: " + tableName, e);
        }
    }

    private Set<String> snapshot(IInvokableInstance instance, String tableName, String tagName)
    {
        Set<String> snapshotDirs = instance.callOnInstance(() -> ColumnFamilyStore.getIfExists(KEYSPACE, tableName)
                                                                                  .snapshot(tagName)
                                                                                  .getDirectories()
                                                                                  .stream()
                                                                                  .map(File::toString)
                                                                                  .collect(Collectors.toSet()));
        assertThat(snapshotDirs).isNotEmpty();
        return snapshotDirs;
    }

    private void createSSTable(String table, String tableArgs)
    {
        try
        {
            CLUSTER.schemaChange(format("CREATE TABLE IF NOT EXISTS %s.%s (pk int, val text, PRIMARY KEY (pk)) %s", KEYSPACE, table, tableArgs));
        }
        catch (Exception e)
        {
            throw new RuntimeException(format("Error creating table %s with args \"%s\"", table, tableArgs), e);
        }
    }

    private void populateTable(String name)
    {
        for (int i = 0; i < 42; i++)
            CLUSTER.get(1).executeInternal(format("INSERT INTO %s.%s (pk, val) VALUES (%s, '%s')", KEYSPACE, name, i, i));
    }
}
