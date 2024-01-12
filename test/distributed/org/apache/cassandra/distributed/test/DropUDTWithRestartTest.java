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

import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.tools.SSTableExport;
import org.apache.cassandra.tools.ToolRunner;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

public class DropUDTWithRestartTest extends TestBaseImpl
{
    @Test
    public void testReadingValuesOfDroppedColumns() throws Throwable
    {
        // given there is a table with a UDT column and some additional non-UDT columns, and there are rows with
        // different combinations of values and nulls for all columns
        try (Cluster cluster = Cluster.build(1).withConfig(c -> c.with(GOSSIP, NATIVE_PROTOCOL)).start())
        {
            IInvokableInstance node = cluster.get(1);
            node.executeInternal("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
            node.executeInternal("CREATE TYPE ks.udt (foo text, bar text)");
            node.executeInternal("CREATE TABLE ks.tab (pk int PRIMARY KEY, a_udt udt, b text, c text)");
            node.executeInternal("INSERT INTO ks.tab (pk, c) VALUES (1, 'c_value')");
            node.executeInternal("INSERT INTO ks.tab (pk, b) VALUES (2, 'b_value')");
            node.executeInternal("INSERT INTO ks.tab (pk, a_udt) VALUES (3, {foo: 'a_foo', bar: 'a_bar'})");

            File dataDir = new File(node.callOnInstance(() -> Keyspace.open("ks")
                                                                      .getColumnFamilyStore("tab")
                                                                      .getDirectories()
                                                                      .getDirectoryForNewSSTables()
                                                                      .absolutePath()));
            checkData(cluster);

            // when the UDT columns is dropped while the data cannot be flushed before drop and must remain in the commitlog

            // prevent flushing the data
            Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(dataDir.toPath());
            permissions.remove(PosixFilePermission.OWNER_WRITE);
            permissions.remove(PosixFilePermission.GROUP_WRITE);
            permissions.remove(PosixFilePermission.OTHERS_WRITE);
            Files.setPosixFilePermissions(dataDir.toPath(), permissions);

            node.executeInternal("ALTER TABLE ks.tab DROP a_udt");

            // and the node is restarted
            // restart is needed because this way we can simulate the situation where the commit log contains the data
            // of the dropped cell, while the schema is already altered (the column moved to dropped columns and transformed)
            node.shutdown(false).get();

            // unlock the ability to flush data
            permissions = Files.getPosixFilePermissions(dataDir.toPath());
            permissions.add(PosixFilePermission.OWNER_WRITE);
            Files.setPosixFilePermissions(dataDir.toPath(), permissions);
            node.startup();

            // then, we should still be able to read the data of the remaining columns correctly
            checkData(cluster);

            // and even after flushing and restarting the node again
            // the next restart is needed to make sure that the sstable header is read from disk
            node.flush("ks");
            node.shutdown(false).get();
            node.startup();

            checkData(cluster);

            // verify that the sstable can be read with sstabledump
            String sstable = node.callOnInstance(() -> Keyspace.open("ks").getColumnFamilyStore("tab")
                                                               .getDirectories().getCFDirectories()
                                                               .get(0).tryList()[0].toString());
            ToolRunner.ToolResult tool = ToolRunner.invokeClass(SSTableExport.class, sstable);
            tool.assertCleanStdErr();
            tool.assertOnExitCode();
            Assertions.assertThat(tool.getStdout())
                      .contains("\"key\" : [ \"1\" ],")
                      .contains("\"key\" : [ \"2\" ],")
                      .contains("{ \"name\" : \"c\", \"value\" : \"c_value\" }")
                      .contains("{ \"name\" : \"b\", \"value\" : \"b_value\" }");
        }
    }

    private void checkData(Cluster cluster)
    {
        ICoordinator coordinator = cluster.coordinator(1);
        String query = "SELECT b, c FROM ks.tab WHERE pk = ?";
        assertRows(coordinator.execute(query, ConsistencyLevel.QUORUM, 1), row(null, "c_value"));
        assertRows(coordinator.execute(query, ConsistencyLevel.QUORUM, 2), row("b_value", null));
        assertRows(coordinator.execute(query, ConsistencyLevel.QUORUM, 3), row(null, null));
    }
}
