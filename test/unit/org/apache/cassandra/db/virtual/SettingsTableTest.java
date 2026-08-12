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

package org.apache.cassandra.db.virtual;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.introspector.Property;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DefaultLoader;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions.Builder;
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions.InternodeEncryption;
import org.apache.cassandra.config.JMXServerOptions;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.config.Properties;
import org.apache.cassandra.config.Redacted;
import org.apache.cassandra.config.SubnetGroups;
import org.apache.cassandra.config.TransparentDataEncryptionOptions;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.repair.autorepair.AutoRepairConfig;
import org.apache.cassandra.security.SSLFactory;
import org.apache.cassandra.utils.JsonUtils;

import static java.util.stream.Collectors.toMap;
import static org.apache.cassandra.config.EncryptionOptions.ClientEncryptionOptions.ClientAuth.REQUIRED;

public class SettingsTableTest extends CQLTester
{
    private static final Logger logger = LoggerFactory.getLogger(SettingsTableTest.class);
    private static final String KS_NAME = "vts";

    private Config config;
    private SettingsTable table;

    @Before
    public void config()
    {
        config = new Config();
        config.client_encryption_options.applyConfig();
        config.server_encryption_options.applyConfig();
        config.jmx_server_options = new JMXServerOptions();
        config.jmx_server_options.jmx_encryption_options.applyConfig();
        config.sstable_preemptive_open_interval = null;
        config.index_summary_resize_interval = null;
        config.cache_load_timeout = new DurationSpec.IntSecondsBound(0);
        config.commitlog_sync_group_window = new DurationSpec.IntMillisecondsBound(0);
        config.credentials_update_interval = null;
        config.data_file_directories = new String[] {"/my/data/directory", "/another/data/directory"};

        Map<String, String> params = new LinkedHashMap<>();
        params.put("keystore_password", "password");
        params.put("key_password", "password");
        params.put("keystore", "conf/.keystore");
        config.transparent_data_encryption_options = new TransparentDataEncryptionOptions(false,
                                                                                          "AES/CBC/PKCS5Padding",
                                                                                          "alias",
                                                                                          new ParameterizedClass("SomeClass",
                                                                                                                 params));

        // populate settings that default to null, so their rendering is exercised by the tests
        // (in particular testCollectionSettingsRenderAsValidJson) instead of being skipped
        config.crypto_provider = new ParameterizedClass("org.apache.cassandra.security.JREProvider",
                                                        Map.of("fail_on_missing_provider", "false"));
        config.internode_authenticator = new ParameterizedClass("org.apache.cassandra.auth.AllowAllInternodeAuthenticator",
                                                                Map.of());
        config.commitlog_compression = new ParameterizedClass("LZ4Compressor",
                                                              Map.of("lz4_compressor_type", "fast"));
        config.hints_compression = new ParameterizedClass("SnappyCompressor",
                                                          Map.of("chunk_length_in_kb", "64"));
        config.default_compaction = new ParameterizedClass("SizeTieredCompactionStrategy",
                                                           Map.of("min_threshold", "4"));
        // populated (rather than default-empty) so the CASSANDRA-21579 fix for SubnetGroups.Group
        // is exercised through the production render path
        config.client_error_reporting_exclusions = new SubnetGroups(List.of("127.0.0.1", "10.110.60.0/26"));
        config.internode_error_reporting_exclusions = new SubnetGroups(List.of("192.168.0.0/16"));
        // likewise for the DurationSpec fix: repair_type_overrides is empty by default, but any node
        // that touches auto-repair populates it via AutoRepairConfig.getOptions(), and a single
        // unserializable nested type makes Jackson fail for the whole setting
        AutoRepairConfig.Options autoRepairOverrides = new AutoRepairConfig.Options();
        autoRepairOverrides.min_repair_interval = new DurationSpec.IntSecondsBound("24h");
        autoRepairOverrides.table_max_repair_time = new DurationSpec.IntSecondsBound("6h");
        config.auto_repair.repair_type_overrides.put("full", autoRepairOverrides);

        table = new SettingsTable(KS_NAME, config);
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, ImmutableList.of(table)));
        disablePreparedReuseForTest();
    }

    @Test
    public void testArray() throws Throwable
    {
        Row one = executeNet("SELECT value FROM vts.settings WHERE name = 'data_file_directories'").one();
        Assert.assertEquals("[\"/my/data/directory\",\"/another/data/directory\"]", one.getString("value"));
    }

    @Test
    public void testSelectAll() throws Throwable
    {
        int paging = (int) (Math.random() * 100 + 1);
        ResultSet result = executeNetWithPaging("SELECT * FROM vts.settings", paging);
        int i = 0;
        for (Row r : result)
        {
            i++;
            String name = r.getString("name");
            Property prop = SettingsTable.PROPERTIES.get(name);
            if (prop != null) // skip overrides
                Assert.assertEquals(table.getValue(prop), r.getString("value"));
        }
        Assert.assertTrue(SettingsTable.PROPERTIES.size() <= i);
    }

    @Test
    public void testSelectPartition() throws Throwable
    {
        for (Map.Entry<String, Property> e : SettingsTable.PROPERTIES.entrySet())
        {
            String name = e.getKey();
            Property prop = e.getValue();
            String q = "SELECT * FROM vts.settings WHERE name = '"+name+'\'';
            assertRowsNet(executeNet(q), new Object[] { name, table.getValue(prop) });
        }
    }

    @Test
    public void testSelectEmpty() throws Throwable
    {
        String q = "SELECT * FROM vts.settings WHERE name = 'EMPTY'";
        assertRowsNet(executeNet(q));
    }

    @Test
    public void testSelectOverride() throws Throwable
    {
        String q = "SELECT * FROM vts.settings WHERE name = 'server_encryption_options_enabled'";
        assertRowsNet(executeNet(q), new Object[] {"server_encryption_options_enabled", "false"});
        q = "SELECT * FROM vts.settings WHERE name = 'server_encryption_options_XYZ'";
        assertRowsNet(executeNet(q));
    }

    @Test
    public void virtualTableBackwardCompatibility() throws Throwable
    {
        // test NEGATIVE_MEBIBYTES_DATA_STORAGE_INT converter
        String q = "SELECT * FROM vts.settings WHERE name = 'sstable_preemptive_open_interval';";
        assertRowsNet(executeNet(q), new Object[] {"sstable_preemptive_open_interval", null});
        q = "SELECT * FROM vts.settings WHERE name = 'sstable_preemptive_open_interval_in_mb';";
        assertRowsNet(executeNet(q), new Object[] {"sstable_preemptive_open_interval_in_mb", "-1"});

        // test MINUTES_CUSTOM_DURATION converter
        q = "SELECT * FROM vts.settings WHERE name = 'index_summary_resize_interval';";
        assertRowsNet(executeNet(q), new Object[] {"index_summary_resize_interval", null});
        q = "SELECT * FROM vts.settings WHERE name = 'index_summary_resize_interval_in_minutes';";
        assertRowsNet(executeNet(q), new Object[] {"index_summary_resize_interval_in_minutes", "-1"});

        // test NEGATIVE_SECONDS_DURATION converter
        q = "SELECT * FROM vts.settings WHERE name = 'cache_load_timeout';";
        assertRowsNet(executeNet(q), new Object[] {"cache_load_timeout", "0s"});
        q = "SELECT * FROM vts.settings WHERE name = 'cache_load_timeout_seconds';";
        assertRowsNet(executeNet(q), new Object[] {"cache_load_timeout_seconds", "0"});

        // test MILLIS_DURATION_DOUBLE converter
        q = "SELECT * FROM vts.settings WHERE name = 'commitlog_sync_group_window';";
        assertRowsNet(executeNet(q), new Object[] {"commitlog_sync_group_window", "0ms"});
        q = "SELECT * FROM vts.settings WHERE name = 'commitlog_sync_group_window_in_ms';";
        assertRowsNet(executeNet(q), new Object[] {"commitlog_sync_group_window_in_ms", "0.0"});

        //test MILLIS_CUSTOM_DURATION converter
        q = "SELECT * FROM vts.settings WHERE name = 'credentials_update_interval';";
        assertRowsNet(executeNet(q), new Object[] {"credentials_update_interval", null});
        q = "SELECT * FROM vts.settings WHERE name = 'credentials_update_interval_in_ms';";
        assertRowsNet(executeNet(q), new Object[] {"credentials_update_interval_in_ms", "-1"});

        // test non matching auth related properties
        q = "SELECT * FROM vts.settings WHERE name = 'authenticator';";
        assertRowsNet(executeNet(q), new Object[] {"authenticator", null});
        q = "SELECT * FROM vts.settings WHERE name = 'authorizer';";
        assertRowsNet(executeNet(q), new Object[] {"authorizer", null});
        q = "SELECT * FROM vts.settings WHERE name = 'network_authorizer';";
        assertRowsNet(executeNet(q), new Object[] {"network_authorizer", null});
        q= "select * from vts.settings where name = 'role_manager';";
        assertRowsNet(executeNet(q), new Object[] {"role_manager", null});
    }

    private void check(String keyspaceTable, String setting, String expected)
    {
        String q = "SELECT * FROM " + keyspaceTable + " WHERE name = '" + setting + '\'';
        try
        {
            assertRowsNet(executeNet(q), new Object[]{ setting, expected });
        }
        catch (AssertionError e)
        {
            throw new AssertionError(e.getMessage() + " for query " + q);
        }
    }

    private void check(String setting, String expected)
    {
        check("vts.settings", setting, expected);
    }

    @Test
    public void testEncryptionOverride() throws Throwable
    {
        String pre = "server_encryption_options_";
        check(pre + "enabled", "false");
        String all = "SELECT * FROM vts.settings WHERE " +
                     "name > 'server_encryption' AND name < 'server_encryptionz' ALLOW FILTERING";

        List<String> expectedNames = SettingsTable.PROPERTIES.keySet().stream().filter(n -> n.startsWith("server_encryption")).collect(Collectors.toList());
        Assert.assertEquals(expectedNames.size(), executeNet(all).all().size());

        Builder serverEncryptionOptionsBuilder = new Builder(config.server_encryption_options);
        check(pre + "algorithm", null);
        config.server_encryption_options = serverEncryptionOptionsBuilder.withAlgorithm("SUPERSSL").build();
        check(pre + "algorithm", "SUPERSSL");

        check(pre + "cipher_suites", null);
        config.server_encryption_options = serverEncryptionOptionsBuilder.withCipherSuites("c1", "c2").build();
        check(pre + "cipher_suites", "[\"c1\",\"c2\"]");

        // name doesn't match yaml
        check(pre + "protocol", null);
        config.server_encryption_options = serverEncryptionOptionsBuilder.withProtocol("TLSv5").build();
        check(pre + "protocol", "[\"TLSv5\"]");

        config.server_encryption_options = serverEncryptionOptionsBuilder.withProtocol("TLS").build();
        try
        {
            check(pre + "protocol", JsonUtils.JSON_OBJECT_MAPPER.writeValueAsString(SSLFactory.tlsInstanceProtocolSubstitution()));
        }
        catch (Exception e)
        {
            throw new RuntimeException("Failed to serialize TLS protocols as JSON", e);
        }

        config.server_encryption_options = serverEncryptionOptionsBuilder.withProtocol("TLS").build();
        config.server_encryption_options = serverEncryptionOptionsBuilder.withAcceptedProtocols(ImmutableList.of("TLSv1.2","TLSv1.1")).build();
        check(pre + "protocol", "[\"TLSv1.2\",\"TLSv1.1\"]");

        config.server_encryption_options = serverEncryptionOptionsBuilder.withProtocol("TLSv2").build();
        config.server_encryption_options = serverEncryptionOptionsBuilder.withAcceptedProtocols(ImmutableList.of("TLSv1.2","TLSv1.1")).build();
        check(pre + "protocol", "[\"TLSv1.2\",\"TLSv1.1\",\"TLSv2\"]"); // protocol goes after the explicit accept list if non-TLS

        check(pre + "optional", "false");
        config.server_encryption_options = serverEncryptionOptionsBuilder.withOptional(true).build();
        check(pre + "optional", "true");

        // name doesn't match yaml
        check(pre + "client_auth", "false");
        config.server_encryption_options = serverEncryptionOptionsBuilder.withRequireClientAuth(REQUIRED).build();
        check(pre + "client_auth", "true");

        // name doesn't match yaml
        check(pre + "endpoint_verification", "false");
        config.server_encryption_options = serverEncryptionOptionsBuilder.withRequireEndpointVerification(true).build();
        check(pre + "endpoint_verification", "true");

        check(pre + "internode_encryption", "none");
        config.server_encryption_options = serverEncryptionOptionsBuilder.withInternodeEncryption(InternodeEncryption.all).build();
        check(pre + "internode_encryption", "all");
        check(pre + "enabled", "true");

        // name doesn't match yaml
        check(pre + "legacy_ssl_storage_port", "false");
        config.server_encryption_options = serverEncryptionOptionsBuilder.withLegacySslStoragePort(true).build();
        check(pre + "legacy_ssl_storage_port", "true");
    }

    @Test
    public void testAuditOverride() throws Throwable
    {
        String pre = "audit_logging_options_";
        check(pre + "enabled", "false");
        String all = "SELECT * FROM vts.settings WHERE " +
                     "name > 'audit_logging' AND name < 'audit_loggingz' ALLOW FILTERING";

        config.audit_logging_options.enabled = true;
        List<String> expectedNames = SettingsTable.PROPERTIES.keySet().stream().filter(n -> n.startsWith("audit_logging")).collect(Collectors.toList());
        Assert.assertEquals(expectedNames.size(), executeNet(all).all().size());
        check(pre + "enabled", "true");

        // name doesn't match yaml
        check(pre + "logger", "BinAuditLogger");
        config.audit_logging_options.logger = new ParameterizedClass("logger", null);
        check(pre + "logger", "logger");

        config.audit_logging_options.audit_logs_dir = "dir";
        check(pre + "audit_logs_dir", "dir");

        check(pre + "included_keyspaces", "");
        config.audit_logging_options.included_keyspaces = "included_keyspaces";
        check(pre + "included_keyspaces", "included_keyspaces");

        check(pre + "excluded_keyspaces", "system,system_schema,system_virtual_schema");
        config.audit_logging_options.excluded_keyspaces = "excluded_keyspaces";
        check(pre + "excluded_keyspaces", "excluded_keyspaces");

        check(pre + "included_categories", "");
        config.audit_logging_options.included_categories = "included_categories";
        check(pre + "included_categories", "included_categories");

        check(pre + "excluded_categories", "");
        config.audit_logging_options.excluded_categories = "excluded_categories";
        check(pre + "excluded_categories", "excluded_categories");

        check(pre + "included_users", "");
        config.audit_logging_options.included_users = "included_users";
        check(pre + "included_users", "included_users");

        check(pre + "excluded_users", "");
        config.audit_logging_options.excluded_users = "excluded_users";
        check(pre + "excluded_users", "excluded_users");
    }

    @Test
    public void testTransparentEncryptionOptionsOverride() throws Throwable
    {
        String pre = "transparent_data_encryption_options_";
        check(pre + "enabled", "false");
        String all = "SELECT * FROM vts.settings WHERE " +
                     "name > 'transparent_data_encryption_options' AND " +
                     "name < 'transparent_data_encryption_optionsz' ALLOW FILTERING";

        config.transparent_data_encryption_options.enabled = true;
        List<String> expectedNames = SettingsTable.PROPERTIES.keySet().stream().filter(n -> n.startsWith("transparent_data_encryption_options")).collect(Collectors.toList());
        Assert.assertEquals(expectedNames.size(), executeNet(all).all().size());
        check(pre + "enabled", "true");

        check(pre + "cipher", "AES/CBC/PKCS5Padding");
        config.transparent_data_encryption_options.cipher = "cipher";
        check(pre + "cipher", "cipher");

        check(pre + "chunk_length_kb", "64");
        config.transparent_data_encryption_options.chunk_length_kb = 5;
        check(pre + "chunk_length_kb", "5");

        check(pre + "iv_length", "16");
        config.transparent_data_encryption_options.iv_length = 7;
        check(pre + "iv_length", "7");
    }

    @Test
    public void testRedaction()
    {
        assertValue("transparent_data_encryption_options.key_provider.parameters",
                    String.format("{\"keystore_password\":\"%s\",\"keystore\":\"conf/.keystore\",\"key_password\":\"%s\"}",
                                  Redacted.REDACTED_STRING,
                                  Redacted.REDACTED_STRING));

        Set<Map.Entry<String, Property>> entries = new DefaultLoader().flatten(Config.class)
                                                                      .entrySet()
                                                                      .stream()
                                                                      .filter(e -> e.getValue().getAnnotation(Redacted.class) != null)
                                                                      .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (e, r) -> e, TreeMap::new))
                                                                      .entrySet();

        Assert.assertFalse(entries.isEmpty());

        for (Map.Entry<String, Property> entry : entries)
        {
            logger.info("redacted {}", entry.getKey());
            assertValue(entry.getKey(), entry.getValue().getAnnotation(Redacted.class).redactedValue());
        }
    }

    private void assertValue(String settingName, String expectedValue)
    {
        List<Row> all = executeNet(String.format("SELECT * from vts.settings WHERE name = '%s'", settingName)).all();
        Assert.assertFalse(all.isEmpty());
        Row row = all.get(0);
        String name = row.getString("name");
        String value = row.getString("value");

        Assert.assertEquals(settingName, name);
        Assert.assertEquals(expectedValue, value);
    }

    @Test
    public void testComplexSettingsFormatProperty()
    {
        Map<String, String> parameters = new HashMap<>();
        parameters.put("seeds", "127.0.0.1:7000");
        config.seed_provider = new ParameterizedClass("org.apache.cassandra.locator.SimpleSeedProvider", parameters);

        // we are not setting property here to true, we expect it to be true by default

        table = new SettingsTable("json_true", config);
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace("json_true", ImmutableList.of(table)));

        check("json_true.settings", "data_file_directories", "[\"/my/data/directory\",\"/another/data/directory\"]");
        check("json_true.settings", "seed_provider.parameters", "{\"seeds\":\"127.0.0.1:7000\"}");
    }

    @Test
    public void testOldBehaviourForComplexSettingsFormatProperty()
    {
        Map<String, String> parameters = new HashMap<>();
        parameters.put("seeds", "127.0.0.1:7000");
        config.seed_provider = new ParameterizedClass("org.apache.cassandra.locator.SimpleSeedProvider", parameters);

        // Test set property to false (collection not as JSON)
        try (WithProperties properties = new WithProperties().set(CassandraRelevantProperties.VIRTUAL_TABLE_COMPLEX_SETTINGS_FORMAT_JSON, "false"))
        {
            table = new SettingsTable("json_false", config);
            VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace("json_false", ImmutableList.of(table)));

            check("json_false.settings", "data_file_directories", "[/my/data/directory, /another/data/directory]");
            check("json_false.settings", "seed_provider.parameters", "{seeds=127.0.0.1:7000}");
        }
    }

    /**
     * Every array/collection/map-valued setting (the only shapes {@link SettingsTable} renders as
     * JSON; scalars are rendered via toString() by design) must produce valid JSON:
     * 1) whatever the current config renders must parse as a JSON array or object, and
     * 2) every element type must be visible to Jackson, including nested properties, so that a
     *    populated value cannot silently fall back to toString() the way SubnetGroups.Group and
     *    the DurationSpec family did (CASSANDRA-21579).
     */
    @Test
    public void testCollectionSettingsRenderAsValidJson()
    {
        // note: normally-null and normally-empty settings are populated in config() so this check
        // exercises their real rendering rather than skipping nulls / serializing empty collections
        Set<String> failures = new TreeSet<>();

        for (Map.Entry<String, Property> e : Properties.defaultLoader().flatten(Config.class).entrySet())
        {
            Property prop = e.getValue();
            Class<?> type = prop.getType();

            List<Class<?>> elements = new ArrayList<>();
            if (type.isArray())
            {
                elements.add(type.getComponentType());
            }
            else if (Collection.class.isAssignableFrom(type) || Map.class.isAssignableFrom(type))
            {
                Class<?>[] args = prop.getActualTypeArguments();
                if (args != null)
                {
                    if (Map.class.isAssignableFrom(type) && args.length == 2)
                        elements.add(args[1]); // keys are stringified by SettingsTable; values serialize as-is
                    else if (args.length >= 1)
                        elements.add(args[0]);
                }
            }
            else
            {
                continue; // scalar settings are rendered via toString() by design
            }

            // 1) the rendered value, through the production path, must be valid JSON
            String rendered = table.getValue(prop);
            if (rendered != null)
            {
                try
                {
                    JsonNode node = JsonUtils.JSON_OBJECT_MAPPER.readTree(rendered);
                    if (!node.isArray() && !node.isObject())
                        failures.add(e.getKey() + ": rendered as neither JSON array nor object: " + rendered);
                }
                catch (Exception ex)
                {
                    failures.add(e.getKey() + ": rendered value is not valid JSON: " + rendered);
                }
            }

            // 2) element types must be Jackson-visible, or a populated value will silently degrade
            for (Class<?> element : elements)
            {
                if (element == null)
                    continue;
                Class<?> nestedType = unrenderableType(element);
                if (nestedType != null)
                    failures.add(e.getKey() + ": element type " + element.getName() + " is not JSON-renderable" +
                                 (nestedType == element ? "" : ", because it reaches " + nestedType.getName()) +
                                 ": no Jackson-visible properties. Annotate that type (e.g. @JsonValue, the way" +
                                 " SubnetGroups.Group and DurationSpec were fixed in CASSANDRA-21579) or this" +
                                 " setting will silently fall back to toString() when populated");
            }
        }

        Assert.assertTrue(String.join("\n", failures), failures.isEmpty());
    }

    /**
     * @return null if Jackson can serialize {@code type}, else the type in its property graph that
     *         Jackson would choke on -- {@code type} itself, or something it reaches
     */
    private static Class<?> unrenderableType(Class<?> type)
    {
        return unrenderableType(type, Collections.newSetFromMap(new IdentityHashMap<>()));
    }

    /**
     * Approximates Jackson's default serialization visibility. A type is renderable if Jackson can
     * discover at least one property on it (or a @JsonValue) and, transitively, every property it
     * discovers is itself renderable. The recursion is the point: {@link AutoRepairConfig.Options} has
     * plenty of visible properties but nests {@link DurationSpec}, and one unserializable nested type
     * makes Jackson throw for the whole setting.
     *
     * This is a bounded approximation. Property types are inspected erased, so a type hidden inside a
     * property's own generic parameters (a {@code Set<Bad>} field, say) is not caught here; check 1 of
     * {@link #testCollectionSettingsRenderAsValidJson} covers that once the setting is populated.
     *
     * @param visiting types on the current path, so a cyclic reference does not recurse forever
     */
    private static Class<?> unrenderableType(Class<?> type, Set<Class<?>> visiting)
    {
        if (type.isArray())
            return unrenderableType(type.getComponentType(), visiting);
        if (type.isPrimitive() || type.isEnum() || type.isInterface())
            return null; // interfaces: the runtime type decides, cannot be judged statically
        if (type.getName().startsWith("java.") || type.getName().startsWith("javax."))
            return null;
        if (CharSequence.class.isAssignableFrom(type) || Number.class.isAssignableFrom(type)
            || Boolean.class == type || Character.class == type)
            return null;
        if (!visiting.add(type))
            return null; // already being checked further up the path; a cycle is not itself a failure

        try
        {
            List<Class<?>> properties = new ArrayList<>();
            for (Method method : type.getMethods())
            {
                if (method.isAnnotationPresent(JsonValue.class))
                    return null;
                if (Modifier.isStatic(method.getModifiers()) || method.getParameterCount() != 0
                    || method.getReturnType() == void.class)
                    continue;
                String name = method.getName();
                if ((name.startsWith("get") && name.length() > 3 && !name.equals("getClass"))
                    || (name.startsWith("is") && name.length() > 2 && (method.getReturnType() == boolean.class || method.getReturnType() == Boolean.class)))
                    properties.add(method.getReturnType());
            }
            for (Field field : type.getFields())
                if (!Modifier.isStatic(field.getModifiers()))
                    properties.add(field.getType());

            if (properties.isEmpty())
                return type; // Jackson discovers nothing to serialize and fails the enclosing value

            for (Class<?> property : properties)
            {
                Class<?> nestedTypes = unrenderableType(property, visiting);
                if (nestedTypes != null)
                    return nestedTypes;
            }

            return null;
        }
        finally
        {
            visiting.remove(type);
        }
    }
}
