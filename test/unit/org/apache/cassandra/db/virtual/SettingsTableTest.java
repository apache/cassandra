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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions.InternodeEncryption;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.security.SSLFactory;

import static org.apache.cassandra.config.DatabaseDescriptor.getPropertyType;
import static org.junit.Assert.assertTrue;

public class SettingsTableTest extends CQLTester
{
    public static final String KS_NAME = "vts";
    private static final Set<String> settingsTableKeys = new HashSet<>();
    private SettingsTable table;
    private Config config;

    @BeforeClass
    public static void setUpClass()
    {
        CQLTester.setUpClass();
        settingsTableKeys.addAll(DatabaseDescriptor.getAllProperties());
        settingsTableKeys.addAll(SettingsTable.BACKWARDS_COMPATABLE_NAMES.keySet());
    }

    @Before
    public void config()
    {
        config = DatabaseDescriptor.getRawConfig();
        config.sstable_preemptive_open_interval = null;
        config.index_summary_resize_interval = null;
        config.cache_load_timeout = new DurationSpec.IntSecondsBound(0);
        config.commitlog_sync_group_window = new DurationSpec.IntMillisecondsBound(0);
        config.credentials_update_interval = null;
        table = new SettingsTable(KS_NAME);
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, ImmutableList.of(table)));
        disablePreparedReuseForTest();
    }

    @Test
    public void testSelectAll() throws Throwable
    {
        int paging = (int) (Math.random() * 100 + 1);
        ResultSet result = executeNetWithPaging("SELECT * FROM vts.settings", paging);
        Set<String> unprocessedKeys = new HashSet<>(settingsTableKeys);
        for (Row r : result)
        {
            String name = r.getString("name");
            String expectedValue = SettingsTable.getPropertyAsString(SettingsTable.getKeyAndWarnIfObsolete(name));
            Assert.assertEquals("Unexpected result for key: " + name, expectedValue, r.getString("value"));
            unprocessedKeys.remove(name);
        }
        Assert.assertTrue("Configuration fields not processed: " + unprocessedKeys, unprocessedKeys.isEmpty());
    }

    @Test
    public void testSelectPartition() throws Throwable
    {
        for (String key : DatabaseDescriptor.getAllProperties())
        {
            String q = "SELECT * FROM vts.settings WHERE name = '" + key + '\'';
            assertRowsNet(executeNet(q),
                          new Object[]{ key,
                                        DatabaseDescriptor.isMutableProperty(key),
                                        SettingsTable.getPropertyAsString(key) });
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
        String q = "SELECT name, value FROM vts.settings WHERE name = 'server_encryption_options_enabled'";
        assertRowsNet(executeNet(q), new Object[] {"server_encryption_options_enabled", "false"});
        q = "SELECT * FROM vts.settings WHERE name = 'server_encryption_options_XYZ'";
        assertRowsNet(executeNet(q));
    }

    @Test
    public void virtualTableBackwardCompatibility() throws Throwable
    {
        // test NEGATIVE_MEBIBYTES_DATA_STORAGE_INT converter
        String q = "SELECT name, value FROM vts.settings WHERE name = 'sstable_preemptive_open_interval';";
        assertRowsNet(executeNet(q), new Object[] {"sstable_preemptive_open_interval", null});
        q = "SELECT name, value FROM vts.settings WHERE name = 'sstable_preemptive_open_interval_in_mb';";
        assertRowsNet(executeNet(q), new Object[] {"sstable_preemptive_open_interval_in_mb", "-1"});

        // test MINUTES_CUSTOM_DURATION converter
        q = "SELECT name, value FROM vts.settings WHERE name = 'index_summary_resize_interval';";
        assertRowsNet(executeNet(q), new Object[] {"index_summary_resize_interval", null});
        q = "SELECT name, value FROM vts.settings WHERE name = 'index_summary_resize_interval_in_minutes';";
        assertRowsNet(executeNet(q), new Object[] {"index_summary_resize_interval_in_minutes", "-1"});

        // test NEGATIVE_SECONDS_DURATION converter
        q = "SELECT name, value FROM vts.settings WHERE name = 'cache_load_timeout';";
        assertRowsNet(executeNet(q), new Object[] {"cache_load_timeout", "0s"});
        q = "SELECT name, value FROM vts.settings WHERE name = 'cache_load_timeout_seconds';";
        assertRowsNet(executeNet(q), new Object[] {"cache_load_timeout_seconds", "0"});

        // test MILLIS_DURATION_DOUBLE converter
        q = "SELECT name, value FROM vts.settings WHERE name = 'commitlog_sync_group_window';";
        assertRowsNet(executeNet(q), new Object[] {"commitlog_sync_group_window", "0ms"});
        q = "SELECT name, value FROM vts.settings WHERE name = 'commitlog_sync_group_window_in_ms';";
        assertRowsNet(executeNet(q), new Object[] {"commitlog_sync_group_window_in_ms", "0.0"});

        //test MILLIS_CUSTOM_DURATION converter
        q = "SELECT name, value FROM vts.settings WHERE name = 'credentials_update_interval';";
        assertRowsNet(executeNet(q), new Object[] {"credentials_update_interval", null});
        q = "SELECT name, value FROM vts.settings WHERE name = 'credentials_update_interval_in_ms';";
        assertRowsNet(executeNet(q), new Object[] {"credentials_update_interval_in_ms", "-1"});
    }

    private void check(String setting, String expected) throws Throwable
    {
        String q = "SELECT name, value FROM vts.settings WHERE name = '"+setting+'\'';
        try
        {
            assertRowsNet(executeNet(q), new Object[] {setting, expected});
        }
        catch (AssertionError e)
        {
            throw new AssertionError(e.getMessage() + " for query " + q);
        }
    }

    @Test
    public void testEncryptionOverride() throws Throwable
    {
        String pre = "server_encryption_options_";
        check(pre + "enabled", "false");
        String all = "SELECT name, value FROM vts.settings WHERE " +
                     "name > 'server_encryption' AND name < 'server_encryptionz' ALLOW FILTERING";

        Set<String> expectedNames = settingsTableKeys.stream().filter(k -> k.startsWith("server_encryption")).collect(Collectors.toSet());
        Assert.assertEquals(expectedNames.size(), executeNet(all).all().size());

        check(pre + "algorithm", null);
        config.server_encryption_options = config.server_encryption_options.withAlgorithm("SUPERSSL");
        check(pre + "algorithm", "SUPERSSL");

        check(pre + "cipher_suites", null);
        config.server_encryption_options = config.server_encryption_options.withCipherSuites("c1", "c2");
        check(pre + "cipher_suites", "[c1, c2]");

        // name doesn't match yaml
        check(pre + "protocol", null);
        config.server_encryption_options = config.server_encryption_options.withProtocol("TLSv5");
        check(pre + "protocol", "[TLSv5]");

        config.server_encryption_options = config.server_encryption_options.withProtocol("TLS");
        check(pre + "protocol", SSLFactory.tlsInstanceProtocolSubstitution().toString());

        config.server_encryption_options = config.server_encryption_options.withProtocol("TLS");
        config.server_encryption_options = config.server_encryption_options.withAcceptedProtocols(ImmutableList.of("TLSv1.2", "TLSv1.1"));
        check(pre + "protocol", "[TLSv1.2, TLSv1.1]");

        config.server_encryption_options = config.server_encryption_options.withProtocol("TLSv2");
        config.server_encryption_options = config.server_encryption_options.withAcceptedProtocols(ImmutableList.of("TLSv1.2", "TLSv1.1"));
        check(pre + "protocol", "[TLSv1.2, TLSv1.1, TLSv2]"); // protocol goes after the explicit accept list if non-TLS

        check(pre + "optional", "false");
        config.server_encryption_options = config.server_encryption_options.withOptional(true);
        check(pre + "optional", "true");

        // name doesn't match yaml
        check(pre + "client_auth", "false");
        config.server_encryption_options = config.server_encryption_options.withRequireClientAuth(true);
        check(pre + "client_auth", "true");

        // name doesn't match yaml
        check(pre + "endpoint_verification", "false");
        config.server_encryption_options = config.server_encryption_options.withRequireEndpointVerification(true);
        check(pre + "endpoint_verification", "true");

        check(pre + "internode_encryption", "none");
        config.server_encryption_options = config.server_encryption_options.withInternodeEncryption(InternodeEncryption.all);
        check(pre + "internode_encryption", "all");
        check(pre + "enabled", "true");

        // name doesn't match yaml
        check(pre + "legacy_ssl_storage_port", "false");
        config.server_encryption_options = config.server_encryption_options.withLegacySslStoragePort(true);
        check(pre + "legacy_ssl_storage_port", "true");
    }

    @Test
    public void testAuditOverride() throws Throwable
    {
        String pre = "audit_logging_options_";
        check(pre + "enabled", "false");
        String all = "SELECT name, value FROM vts.settings WHERE " +
                     "name > 'audit_logging' AND name < 'audit_loggingz' ALLOW FILTERING";

        config.audit_logging_options.enabled = true;
        Set<String> expectedNames = settingsTableKeys.stream().filter(k -> k.startsWith("audit_logging")).collect(Collectors.toSet());
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
        String all = "SELECT name, value FROM vts.settings WHERE " +
                     "name > 'transparent_data_encryption_options' AND " +
                     "name < 'transparent_data_encryption_optionsz' ALLOW FILTERING";

        config.transparent_data_encryption_options.enabled = true;
        Set<String> expectedNames = settingsTableKeys.stream().filter(k -> k.startsWith("transparent_data_encryption_options")).collect(Collectors.toSet());
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
    public void testCompareYamlRepresentWithPropertyToString()
    {
        List<String> properties = new ArrayList<>();
        for (String name : DatabaseDescriptor.getAllProperties())
        {
            String propertyValueString = SettingsTable.getPropertyAsString(name);
            if (propertyValueString == null)
                return;

            // There are some exceptions where the property value is not the same as the yaml representation
            // e.g. the property value is a Set but the yaml representation of a Set differs from the toString() of a Set.
            // toString() of an empty Set is "[]" but the yaml representation of an empty Set is "{}".
            if (Set.class.equals(getPropertyType(name)) && ((Set<?>) DatabaseDescriptor.getPropertyValue(name)).isEmpty())
                return;

            String expected = DatabaseDescriptor.getPropertyValue(name).toString();
            if (!propertyValueString.equals(expected))
            {
                String out = String.format("Property '%s' expected:\n %s\n" +
                                           "Property '%s' actual:\n %s\n",
                                           name, expected, name, propertyValueString);
                // Here are some exceptions where the property value is not the same as the yaml representation.
                if (name.equals("memtable.configurations") ||
                    name.equals("sstable_formats") ||
                    name.equals("seed_provider.parameters") ||
                    name.equals("data_file_directories"))
                    System.err.println(out);
                else
                    properties.add(out);
            }
        }
        assertTrue('\n' + String.join("------------------------------------------------\n", properties),
                   properties.isEmpty());
    }
}
