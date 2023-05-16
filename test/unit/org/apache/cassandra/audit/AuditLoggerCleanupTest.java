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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.stream.Stream;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.table.SingleTableStore;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.OverrideConfigurationLoader;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.cassandra.service.StorageService;

import static java.nio.file.Files.list;
import static org.apache.cassandra.config.CassandraRelevantProperties.SUPERUSER_SETUP_DELAY_MS;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AuditLoggerCleanupTest
{
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private EmbeddedCassandraService embedded;
    private File auditLogDirRoot;
    private File emptyCq4File;
    private File emptyMetadataFile;

    @Before
    public void setup() throws Exception
    {
        OverrideConfigurationLoader.override((config) -> {
            config.audit_logging_options.enabled = true;
            config.audit_logging_options.audit_logs_dir = auditLogDirRoot.getAbsolutePath();
            config.audit_logging_options.roll_cycle = "MINUTELY";
        });

        auditLogDirRoot = temporaryFolder.getRoot();

        // invalid file will be removed on startup
        emptyCq4File = Files.createFile(auditLogDirRoot.toPath().resolve("20220928-12" + SingleChronicleQueue.SUFFIX)).toFile();
        emptyMetadataFile = Files.createFile(auditLogDirRoot.toPath().resolve("metadata" + SingleTableStore.SUFFIX)).toFile();

        SUPERUSER_SETUP_DELAY_MS.setLong(0);

        embedded = new EmbeddedCassandraService();
        embedded.start();
    }

    @After
    public void shutdown()
    {
        embedded.stop();
    }

    @Test
    public void testCleanupOfAuditLogDir() throws Throwable
    {
        // node started even there was empty cq4 file as it was removed upon start
        assertTrue(StorageService.instance.isAuditLogEnabled());
        assertFalse(emptyCq4File.exists());
        // empty metadata file is reused
        assertTrue(emptyMetadataFile.exists() && emptyMetadataFile.length() != 0);

        insertData();

        assertLogFileExists();

        StorageService.instance.disableAuditLog();

        // disabling and enabling from JMX will trigger same empty file cleanup

        emptyCq4File = Files.createFile(auditLogDirRoot.toPath().resolve("20220928-12" + SingleChronicleQueue.SUFFIX)).toFile();

        StorageService.instance.enableAuditLog(null, null, null, null, null, null, null, null);

        assertTrue(StorageService.instance.isAuditLogEnabled());

        // invalid file were removed again
        assertFalse(emptyCq4File.exists());
        // only valid files are present
        assertLogFileExists();
    }

    private void assertLogFileExists() throws Exception
    {
        try (Stream<Path> stream = list(auditLogDirRoot.toPath()))
        {
            assertTrue(stream.allMatch(p -> {
                String fileName = p.getFileName().toString();
                return (fileName.endsWith(SingleChronicleQueue.SUFFIX) || fileName.endsWith(SingleTableStore.SUFFIX))
                       && p.toFile().isFile() && p.toFile().length() != 0;
            }));
        }
    }

    private void insertData()
    {
        String keyspaceName = "c17933_" + UUID.randomUUID().toString().replace("-", "");
        execute("CREATE KEYSPACE " + keyspaceName + "  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};");
        execute("CREATE TABLE " + keyspaceName + ".tb1 (id int primary key);");
        execute("INSERT INTO " + keyspaceName + ".tb1 (id) VALUES (1)");
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
