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
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions.InternodeEncryption;
import org.apache.cassandra.cql3.CQLTester;

public class SetttingsTableTest extends CQLTester
{
    private static final String KS_NAME = "vts";
    private static SettingsTable table;

    @BeforeClass
    public static void setUpClass()
    {
        table = new SettingsTable(KS_NAME);
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, ImmutableList.of(table)));
        CQLTester.setUpClass();
    }

    @Before
    public void config()
    {
        table.config = new Config();
    }

    private String getValue(Field f)
    {
        Object untypedValue = table.getValue(f);
        String value = null;
        if (untypedValue != null)
        {
            if (untypedValue.getClass().isArray())
            {
                value = Arrays.toString((Object[]) untypedValue);
            }
            else
                value = untypedValue.toString();
        }
        return value;
    }

    @Test
    public void testSelectAll() throws Throwable
    {

        int paging = (int) (Math.random() * 100 + 1);
        ResultSet result = executeNetWithPaging("SELECT * FROM vts.settings", paging);
        int i = 0;
        for (Row r : result)
        {
            i ++;
            String setting = r.getString("setting");
            Field f = SettingsTable.FIELDS.get(setting);
            if (f == null) // skip overrides
                continue;
            Assert.assertEquals(getValue(f), r.getString("value"));
        }
        Assert.assertEquals(SettingsTable.FIELDS.size(), i);
    }

    @Test
    public void testSelectPartition() throws Throwable
    {
        List<Field> fields = Arrays.stream(Config.class.getFields())
                                   .filter(f -> !Modifier.isStatic(f.getModifiers()))
                                   .collect(Collectors.toList());
        for (Field f : fields)
        {
            if (table.overrides.containsKey(f.getName()))
                continue;

            String q = "SELECT * FROM vts.settings WHERE setting = '"+f.getName()+'\'';
            assertRowsNet(executeNet(q), new Object[] {f.getName(), getValue(f)});
        }
    }

    @Test
    public void testSelectEmpty() throws Throwable
    {
        String q = "SELECT * FROM vts.settings WHERE setting = 'EMPTY'";
        assertRowsNet(executeNet(q));
    }

    @Test
    public void testSelectOverride() throws Throwable
    {
        String q = "SELECT * FROM vts.settings WHERE setting = 'server_encryption_options_enabled'";
        assertRowsNet(executeNet(q), new Object[] {"server_encryption_options_enabled", "false"});
        q = "SELECT * FROM vts.settings WHERE setting = 'server_encryption_options_XYZ'";
        assertRowsNet(executeNet(q));
    }

    private void check(String setting, String expected) throws Throwable
    {
        String q = "SELECT * FROM vts.settings WHERE setting = '"+setting+'\'';
        assertRowsNet(executeNet(q), new Object[] {setting, expected});
    }

    @Test
    public void testEncryptionOverride() throws Throwable
    {
        String pre = "server_encryption_options_";
        check(pre + "enabled", "false");
        String algo = "SELECT * FROM vts.settings WHERE setting = 'server_encryption_options_algorithm'";
        String all = "SELECT * FROM vts.settings WHERE " +
                     "setting > 'server_encryption' AND setting < 'server_encryptionz' ALLOW FILTERING";
        assertRowsNet(executeNet(algo));

        Assert.assertEquals(1, executeNet(all).all().size());
        table.config.server_encryption_options.enabled = true;
        Assert.assertEquals(9, executeNet(all).all().size());
        check(pre + "enabled", "true");

        check(pre + "algorithm", null);
        table.config.server_encryption_options.algorithm = "SUPERSSL";
        check(pre + "algorithm", "SUPERSSL");

        check(pre + "cipher_suites", "[]");
        table.config.server_encryption_options.cipher_suites = new String[]{"c1", "c2"};
        check(pre + "cipher_suites", "[c1, c2]");

        check(pre + "protocol", table.config.server_encryption_options.protocol);
        table.config.server_encryption_options.protocol = "TLSv5";
        check(pre + "protocol", "TLSv5");

        check(pre + "optional", "false");
        table.config.server_encryption_options.optional = true;
        check(pre + "optional", "true");

        check(pre + "client_auth", "false");
        table.config.server_encryption_options.require_client_auth = true;
        check(pre + "client_auth", "true");

        check(pre + "endpoint_verification", "false");
        table.config.server_encryption_options.require_endpoint_verification = true;
        check(pre + "endpoint_verification", "true");

        check(pre + "internode_encryption", "none");
        table.config.server_encryption_options.internode_encryption = InternodeEncryption.all;
        check(pre + "internode_encryption", "all");

        check(pre + "legacy_ssl_storage_port", "false");
        table.config.server_encryption_options.enable_legacy_ssl_storage_port = true;
        check(pre + "legacy_ssl_storage_port", "true");
    }

    @Test
    public void testAuditOverride() throws Throwable
    {
        String pre = "audit_logging_options_";
        check(pre + "enabled", "false");
        String logger = "SELECT * FROM vts.settings WHERE setting = 'audit_logging_options_logger'";
        String all = "SELECT * FROM vts.settings WHERE " +
                     "setting > 'audit_logging' AND setting < 'audit_loggingz' ALLOW FILTERING";

        assertRowsNet(executeNet(logger));
        Assert.assertEquals(1, executeNet(all).all().size());
        table.config.audit_logging_options.enabled = true;
        Assert.assertEquals(9, executeNet(all).all().size());
        check(pre + "enabled", "true");

        check(pre + "logger", "BinAuditLogger");
        table.config.audit_logging_options.logger = "logger";
        check(pre + "logger", "logger");

        table.config.audit_logging_options.audit_logs_dir = "dir";
        check(pre + "audit_logs_dir", "dir");

        check(pre + "included_keyspaces", "");
        table.config.audit_logging_options.included_keyspaces = "included_keyspaces";
        check(pre + "included_keyspaces", "included_keyspaces");

        check(pre + "excluded_keyspaces", "");
        table.config.audit_logging_options.excluded_keyspaces = "excluded_keyspaces";
        check(pre + "excluded_keyspaces", "excluded_keyspaces");

        check(pre + "included_categories", "");
        table.config.audit_logging_options.included_categories = "included_categories";
        check(pre + "included_categories", "included_categories");

        check(pre + "excluded_categories", "");
        table.config.audit_logging_options.excluded_categories = "excluded_categories";
        check(pre + "excluded_categories", "excluded_categories");

        check(pre + "included_users", "");
        table.config.audit_logging_options.included_users = "included_users";
        check(pre + "included_users", "included_users");

        check(pre + "excluded_users", "");
        table.config.audit_logging_options.excluded_users = "excluded_users";
        check(pre + "excluded_users", "excluded_users");
    }

    @Test
    public void testTransparentEncryptionOptionsOverride() throws Throwable
    {
        String pre = "transparent_data_encryption_options_";
        check(pre + "enabled", "false");
        String cipher = "SELECT * FROM vts.settings WHERE setting = 'transparent_data_encryption_options_cipher'";
        String all = "SELECT * FROM vts.settings WHERE " +
                     "setting > 'transparent_data_encryption_options' AND " +
                     "setting < 'transparent_data_encryption_optionsz' ALLOW FILTERING";

        assertRowsNet(executeNet(cipher));
        Assert.assertEquals(1, executeNet(all).all().size());
        table.config.transparent_data_encryption_options.enabled = true;
        Assert.assertEquals(4, executeNet(all).all().size());
        check(pre + "enabled", "true");

        check(pre + "cipher", "AES/CBC/PKCS5Padding");
        table.config.transparent_data_encryption_options.cipher = "cipher";
        check(pre + "cipher", "cipher");

        check(pre + "chunk_length_kb", "64");
        table.config.transparent_data_encryption_options.chunk_length_kb = 5;
        check(pre + "chunk_length_kb", "5");

        check(pre + "iv_length", "16");
        table.config.transparent_data_encryption_options.iv_length = 7;
        check(pre + "iv_length", "7");
    }
}
