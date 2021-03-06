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

import java.nio.file.Path;

import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.RollCycles;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.utils.binlog.BinLogTest;

import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class BinAuditLoggerTest extends CQLTester
{
    private static Path tempDir;

    @BeforeClass
    public static void setUp() throws Exception
    {
        tempDir = BinLogTest.tempDir();

        AuditLogOptions options = new AuditLogOptions();
        options.enabled = true;
        options.logger = new ParameterizedClass("BinAuditLogger", null);
        options.roll_cycle = "TEST_SECONDLY";
        options.audit_logs_dir = tempDir.toString();
        DatabaseDescriptor.setAuditLoggingOptions(options);
        AuditLogManager.instance.enable(DatabaseDescriptor.getAuditLoggingOptions());
        requireNetwork();
    }

    @Test
    public void testSelectRoundTripQuery() throws Throwable
    {
        createTable("CREATE TABLE %s (id int primary key, v1 text, v2 text)");
        execute("INSERT INTO %s (id, v1, v2) VALUES (?, ?, ?)", 1, "Apache", "Cassandra");
        execute("INSERT INTO %s (id, v1, v2) VALUES (?, ?, ?)", 2, "trace", "test");

        String cql = "SELECT id, v1, v2 FROM " + KEYSPACE + '.' + currentTable() + " WHERE id = ?";

        Session session = sessionNet();

        PreparedStatement pstmt = session.prepare(cql);
        ResultSet rs = session.execute(pstmt.bind(1));

        assertEquals(1, rs.all().size());
        try (ChronicleQueue queue = SingleChronicleQueueBuilder.single(tempDir.toFile()).rollCycle(RollCycles.TEST_SECONDLY).build())
        {
            ExcerptTailer tailer = queue.createTailer();
            assertTrue(tailer.readDocument(wire -> {
                assertEquals(0L, wire.read("version").int16());
                assertEquals("audit", wire.read("type").text());
                assertThat(wire.read("message").text(), containsString(AuditLogEntryType.PREPARE_STATEMENT.toString()));
            }));

            assertTrue(tailer.readDocument(wire -> {
                assertEquals(0L, wire.read("version").int16());
                assertEquals("audit", wire.read("type").text());
                assertThat(wire.read("message").text(), containsString(AuditLogEntryType.SELECT.toString()));
            }));
            assertFalse(tailer.readDocument(wire -> {
            }));
        }
    }
}
