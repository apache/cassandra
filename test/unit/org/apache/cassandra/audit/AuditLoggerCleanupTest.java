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

package org.apache.cassandra.audit;

import java.io.File;
import java.net.InetAddress;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.OverrideConfigurationLoader;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.cassandra.service.StorageService;

import static java.nio.file.Files.list;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AuditLoggerCleanupTest
{
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();
    private EmbeddedCassandraService embedded;

    private File emptyCq4File;

    @Before
    public void setup() throws Exception
    {
        OverrideConfigurationLoader.override((config) -> {
            config.audit_logging_options.enabled = true;
            config.audit_logging_options.audit_logs_dir = temporaryFolder.getRoot().getAbsolutePath();
            config.audit_logging_options.roll_cycle = "MINUTELY";
        });

        // invalid file will be removed on startup
        emptyCq4File = temporaryFolder.newFile("20220928-12.cq4");

        CQLTester.prepareServer();

        System.setProperty("cassandra.superuser_setup_delay_ms", "0");
        embedded = new EmbeddedCassandraService();
        embedded.start();
    }

    @After
    public void shutdown()
    {
        embedded.stop();
        System.clearProperty(Config.PROPERTY_PREFIX + "config.loader");
    }

    @Test
    public void test() throws Throwable
    {
        // node started even there was empty cq4 file as it was removed upon start
        assertTrue(StorageService.instance.isAuditLogEnabled());
        assertFalse(emptyCq4File.exists());

        insertData();

        assertLogFileExists();

        StorageService.instance.disableAuditLog();

        // disabling and enabling from JMX will trigger same empty file cleanup

        emptyCq4File = temporaryFolder.newFile("20220928-12.cq4");

        StorageService.instance.enableAuditLog(null, null, null, null, null, null, null, null);

        assertTrue(StorageService.instance.isAuditLogEnabled());

        // invalid file were removed again
        assertFalse(emptyCq4File.exists());
        // only valid files are present
        assertLogFileExists();
    }

    private void assertLogFileExists() throws Exception
    {
        assertTrue(list(temporaryFolder.getRoot().toPath()).allMatch(p -> {
            String fileName = p.getFileName().toString();
            return fileName.endsWith("cq4") || fileName.endsWith("cq4t");
        }));
    }

    private void insertData()
    {
        execute("CREATE KEYSPACE ks1 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};");
        execute("CREATE TABLE ks1.tb1 (id int primary key);");
        execute("INSERT INTO ks1.tb1 (id) VALUES (1)");
    }

    private static void execute(String query)
    {
        try (
        Cluster cluster = Cluster.builder().addContactPoints(InetAddress.getLoopbackAddress())
                                 .withoutJMXReporting()
                                 .withPort(DatabaseDescriptor.getNativeTransportPort()).build())
        {
            try (Session session = cluster.connect())
            {
                session.execute(query);
            }
        }
    }
}
