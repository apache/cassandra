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

package org.apache.cassandra.tools.nodetool;

import java.net.InetAddress;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tools.ToolRunner;
import org.assertj.core.groups.Tuple;

import static org.apache.cassandra.config.CassandraRelevantProperties.CASSANDRA_JMX_LOCAL_PORT;
import static org.assertj.core.api.Assertions.assertThat;

public class ClientStatsTest
{
    private static Cluster cluster;
    private Session session;

    private static EmbeddedCassandraService cassandra;

    @BeforeClass
    public static void setup() throws Throwable
    {
        // Since we run EmbeddedCassandraServer, we need to manually associate JMX address; otherwise it won't start
        int jmxPort = CQLTester.getAutomaticallyAllocatedPort(InetAddress.getLoopbackAddress());
        CASSANDRA_JMX_LOCAL_PORT.setInt(jmxPort);

        cassandra = ServerTestUtils.startEmbeddedCassandraService();
        cluster = Cluster.builder().addContactPoint("127.0.0.1").withPort(DatabaseDescriptor.getNativeTransportPort()).build();
    }

    @Before
    public void config() throws Throwable
    {
        session = cluster.connect();
        ResultSet result = session.execute("select release_version from system.local");
    }

    @After
    public void afterTest()
    {
        if (session != null)
            session.close();
    }

    @AfterClass
    public static void tearDown()
    {
        if (cluster != null)
            cluster.close();
        if (cassandra != null)
            cassandra.stop();
    }

    @Test
    public void testClientStatsHelp()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("help", "clientstats");
        tool.assertOnCleanExit();

        String help = "NAME\n" +
                      "        nodetool clientstats - Print information about connected clients\n" +
                      "\n" +
                      "SYNOPSIS\n" +
                      "        nodetool [(-h <host> | --host <host>)] [(-p <port> | --port <port>)]\n" +
                      "                [(-pp | --print-port)] [(-pw <password> | --password <password>)]\n" +
                      "                [(-pwf <passwordFilePath> | --password-file <passwordFilePath>)]\n" +
                      "                [(-u <username> | --username <username>)] clientstats [--all]\n" +
                        "                [--by-protocol] [--clear-history] [--client-options]\n" +
                      "\n" +
                      "OPTIONS\n" +
                      "        --all\n" +
                      "            Lists all connections\n" +
                      "\n" +
                      "        --by-protocol\n" +
                      "            Lists most recent client connections by protocol version\n" +
                      "\n" +
                      "        --clear-history\n" +
                      "            Clear the history of connected clients\n" +
                        "\n" +
                        "        --client-options\n" +
                        "            Lists all connections and the client options\n" +
                      "\n" +
                      "        -h <host>, --host <host>\n" +
                      "            Node hostname or ip address\n" +
                      "\n" +
                      "        -p <port>, --port <port>\n" +
                      "            Remote jmx agent port number\n" +
                      "\n" +
                      "        -pp, --print-port\n" +
                      "            Operate in 4.0 mode with hosts disambiguated by port number\n" +
                      "\n" +
                      "        -pw <password>, --password <password>\n" +
                      "            Remote jmx agent password\n" +
                      "\n" +
                      "        -pwf <passwordFilePath>, --password-file <passwordFilePath>\n" +
                      "            Path to the JMX password file\n" +
                      "\n" +
                      "        -u <username>, --username <username>\n" +
                      "            Remote jmx agent username\n" +
                      "\n" +
                      "\n";
        assertThat(tool.getStdout()).isEqualTo(help);
    }

    @Test
    public void testClientStats()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("clientstats");
        tool.assertOnCleanExit();
        String stdout = tool.getStdout();
        assertThat(stdout).contains("Total connected clients: 2");
        assertThat(stdout).contains("User      Connections");
        assertThat(stdout).contains("anonymous 2");
    }

    @Test
    public void testClientStatsByProtocol()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("clientstats", "--by-protocol");
        tool.assertOnCleanExit();
        String stdout = tool.getStdout();
        assertThat(stdout).contains("Clients by protocol version");
        assertThat(stdout).contains("Protocol-Version IP-Address Last-Seen");
        assertThat(stdout).containsPattern("[0-9]/v[0-9] +/127.0.0.1 [a-zA-Z]{3} [0-9]+, [0-9]{4} [0-9]{2}:[0-9]{2}:[0-9]{2}");
    }

    @Test
    public void testClientStatsAll()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("clientstats", "--all");
        tool.assertOnCleanExit();
        String stdout = tool.getStdout();
        assertThat(stdout).containsPattern("Address +SSL +Cipher +Protocol +Version +User +Keyspace +Requests +Driver-Name +Driver-Version");
        assertThat(stdout).containsPattern("/127.0.0.1:[0-9]+ false undefined undefined [0-9]+ +anonymous +[0-9]+ +DataStax Java Driver 3.11.5");
        assertThat(stdout).contains("Total connected clients: 2");
        assertThat(stdout).contains("User      Connections");
        assertThat(stdout).contains("anonymous 2");
    }

    @Test
    public void testClientStatsClientOptions()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("clientstats", "--client-options");
        tool.assertOnCleanExit();
        String stdout = tool.getStdout();
        assertThat(stdout).containsPattern("Address +SSL +Cipher +Protocol +Version +User +Keyspace +Requests +Driver-Name +Driver-Version +Client-Options");
        assertThat(stdout).containsPattern("/127.0.0.1:[0-9]+ false undefined undefined [0-9]+ +anonymous +[0-9]+ +DataStax Java Driver 3.11.5");
        assertThat(stdout).containsPattern("DRIVER_NAME=DataStax Java Driver");
        assertThat(stdout).containsPattern("DRIVER_VERSION=3.11.5");
        assertThat(stdout).containsPattern("CQL_VERSION=3.0.0");
        assertThat(stdout).contains("Total connected clients: 2");
        assertThat(stdout).contains("User      Connections");
        assertThat(stdout).contains("anonymous 2");
    }

    @Test
    public void testClientStatsClearHistory()
    {
        ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
        Logger ssLogger = (Logger) LoggerFactory.getLogger(StorageService.class);

        ssLogger.addAppender(listAppender);
        listAppender.start();

        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("clientstats", "--clear-history");
        tool.assertOnCleanExit();
        String stdout = tool.getStdout();
        assertThat(stdout).contains("Clearing connection history");
        assertThat(listAppender.list)
        .extracting(ILoggingEvent::getMessage, ILoggingEvent::getLevel)
        .contains(Tuple.tuple("Cleared connection history", Level.INFO));
    }
}
