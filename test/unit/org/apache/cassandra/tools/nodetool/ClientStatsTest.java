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
import com.datastax.driver.core.Session;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.OverrideConfigurationLoader;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tools.ToolRunner;
import org.apache.cassandra.transport.TlsTestUtils;
import org.assertj.core.groups.Tuple;

import static org.apache.cassandra.auth.AuthTestUtils.waitForExistingRoles;
import static org.apache.cassandra.config.CassandraRelevantProperties.CASSANDRA_JMX_LOCAL_PORT;
import static org.apache.cassandra.config.CassandraRelevantProperties.SUPERUSER_SETUP_DELAY_MS;
import static org.assertj.core.api.Assertions.assertThat;

public class ClientStatsTest
{
    private static Cluster cluster;

    private static Cluster tlsCluster;

    private static Session tlsSession;

    private static Cluster mtlsCluster;

    private static Session mtlsSession;

    private Session session;

    private static EmbeddedCassandraService cassandra;

    @BeforeClass
    public static void setup() throws Throwable
    {
        OverrideConfigurationLoader.override(TlsTestUtils::configureWithMutualTlsWithPasswordFallbackAuthenticator);

        SUPERUSER_SETUP_DELAY_MS.setLong(0);
        // Since we run EmbeddedCassandraServer, we need to manually associate JMX address; otherwise it won't start
        int jmxPort = CQLTester.getAutomaticallyAllocatedPort(InetAddress.getLoopbackAddress());
        CASSANDRA_JMX_LOCAL_PORT.setInt(jmxPort);

        cassandra = ServerTestUtils.startEmbeddedCassandraService();

        waitForExistingRoles();

        cluster = clusterBuilder()
                  .withCredentials("cassandra", "cassandra")
                  .build();

        // Allow client to connect as cassandra using an mTLS identity.
        try(Session session = cluster.connect())
        {
            session.execute(String.format("ADD IDENTITY '%s' TO ROLE 'cassandra'", TlsTestUtils.CLIENT_SPIFFE_IDENTITY));
        }

        // Configure a TLS-based cluster with password authentication.
        tlsCluster =  clusterBuilder()
                             .withSSL(TlsTestUtils.getSSLOptions(false))
                             .withCredentials("cassandra", "cassandra")
                             .build();

        // Configure a TLS-based cluster with certificate (mtls) authentication.
        mtlsCluster =  clusterBuilder()
                              .withSSL(TlsTestUtils.getSSLOptions(true))
                              .build();
    }

    private static Cluster.Builder clusterBuilder()
    {
        return Cluster.builder().addContactPoint("127.0.0.1").withPort(DatabaseDescriptor.getNativeTransportPort());
    }

    @Before
    public void config() throws Throwable
    {

        session = cluster.connect();
        session.execute("select release_version from system.local");

        tlsSession = tlsCluster.connect();
        // connect with system keyspace to assert it's present in output.
        mtlsSession = mtlsCluster.connect("system");
    }

    @After
    public void afterTest()
    {
        if (session != null)
            session.close();
        if (tlsSession != null)
            tlsSession.close();
        if (mtlsSession != null)
            mtlsSession.close();
    }

    @AfterClass
    public static void tearDown()
    {
        if (cluster != null)
            cluster.close();
        if (tlsCluster != null)
            tlsCluster.close();
        if (mtlsCluster != null)
            mtlsCluster.close();
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
                      "                [--by-protocol] [--clear-history] [--client-options] [--verbose]\n" +
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
                      "        --verbose\n" +
                      "            Lists all connections with additional details (client options,\n" +
                      "            authenticator-specific metadata and more)\n" +
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
        assertClientCount(stdout);
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
        /**
         * Example expected output:
         * Address          SSL   Cipher                 Protocol  Version User      Keyspace Requests Driver-Name          Driver-Version
         * /127.0.0.1:52549 true  TLS_AES_256_GCM_SHA384 TLSv1.3   5       cassandra          17       DataStax Java Driver 3.11.5
         * /127.0.0.1:52550 true  TLS_AES_256_GCM_SHA384 TLSv1.3   5       cassandra          3        DataStax Java Driver 3.11.5
         * /127.0.0.1:52551 true  TLS_AES_256_GCM_SHA384 TLSv1.3   5       cassandra          16       DataStax Java Driver 3.11.5
         * /127.0.0.1:52552 true  TLS_AES_256_GCM_SHA384 TLSv1.3   5       cassandra system   3        DataStax Java Driver 3.11.5
         * /127.0.0.1:52546 false undefined              undefined 5       cassandra          17       DataStax Java Driver 3.11.5
         * /127.0.0.1:52548 false undefined              undefined 5       cassandra          4        DataStax Java Driver 3.11.5
         */
        assertThat(stdout).containsPattern("Address +SSL +Cipher +Protocol +Version +User +Keyspace +Requests +Driver-Name +Driver-Version");
        // Unencrypted password-based client.
        assertThat(stdout).containsPattern("/127.0.0.1:[0-9]+ false +undefined +undefined +[0-9]+ +cassandra +[0-9]+ +DataStax Java Driver 3.11.5");
        // TLS-encrypted password-based client.
        assertThat(stdout).containsPattern("/127.0.0.1:[0-9]+ true +TLS\\S+ +TLS\\S+ +[0-9]+ +cassandra +[0-9]+ +DataStax Java Driver 3.11.5");
        // MTLS-based client.
        assertThat(stdout).containsPattern("/127.0.0.1:[0-9]+ true +TLS\\S+ +TLS\\S+ +[0-9]+ +cassandra +[0-9]+ +DataStax Java Driver 3.11.5");
        // MTLS-based client with 'system' keyspace set on connection.
        assertThat(stdout).containsPattern("/127.0.0.1:[0-9]+ true +TLS\\S+ +TLS\\S+ +[0-9]+ +cassandra +system +[0-9]+ +DataStax Java Driver 3.11.5");

        assertClientCount(stdout);
    }

    @Test
    public void testClientStatsClientOptions()
    {
        // given 'clientstats --metadata' invoked, we expect 'Client-Options' to be present.
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("clientstats", "--client-options");
        tool.assertOnCleanExit();
        String stdout = tool.getStdout();

        /*
         * Example expected output:
         * Address          SSL   Cipher                 Protocol  Version User      Keyspace Requests Driver-Name          Driver-Version Client-Options
         * /127.0.0.1:51047 true  TLS_AES_256_GCM_SHA384 TLSv1.3   5       cassandra          17       DataStax Java Driver 3.11.5         DRIVER_VERSION=3.11.5, DRIVER_NAME=DataStax Java Driver, CQL_VERSION=3.0.0
         * /127.0.0.1:51048 true  TLS_AES_256_GCM_SHA384 TLSv1.3   5       cassandra          3        DataStax Java Driver 3.11.5         DRIVER_VERSION=3.11.5, DRIVER_NAME=DataStax Java Driver, CQL_VERSION=3.0.0
         * /127.0.0.1:51046 false undefined              undefined 5       cassandra          4        DataStax Java Driver 3.11.5         DRIVER_VERSION=3.11.5, DRIVER_NAME=DataStax Java Driver, CQL_VERSION=3.0.0
         * /127.0.0.1:51044 false undefined              undefined 5       cassandra          17       DataStax Java Driver 3.11.5         DRIVER_VERSION=3.11.5, DRIVER_NAME=DataStax Java Driver, CQL_VERSION=3.0.0
         * /127.0.0.1:51049 true  TLS_AES_256_GCM_SHA384 TLSv1.3   5       cassandra          16       DataStax Java Driver 3.11.5         DRIVER_VERSION=3.11.5, DRIVER_NAME=DataStax Java Driver, CQL_VERSION=3.0.0
         * /127.0.0.1:51050 true  TLS_AES_256_GCM_SHA384 TLSv1.3   5       cassandra system   3        DataStax Java Driver 3.11.5         DRIVER_VERSION=3.11.5, DRIVER_NAME=DataStax Java Driver, CQL_VERSION=3.0.0
         */
        assertThat(stdout).containsPattern("Address +SSL +Cipher +Protocol +Version +User +Keyspace +Requests +Driver-Name +Driver-Version +Client-Options");
        assertThat(stdout).containsPattern("/127.0.0.1:[0-9]+ false+ undefined +undefined +[0-9]+ +cassandra +[0-9]+ +DataStax Java Driver 3.11.5");
        assertThat(stdout).containsPattern("DRIVER_NAME=DataStax Java Driver");
        assertThat(stdout).containsPattern("DRIVER_VERSION=3.11.5");
        assertThat(stdout).containsPattern("CQL_VERSION=3.0.0");

        assertClientCount(stdout);
    }

    @Test
    public void testClientStatsClientVerbose()
    {
        // given 'clientstats --verbose' invoked, we expect 'Client-Options', 'Auth-Mode', 'Auth-Metadata', and 'Client-Options' columns to be present.
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("clientstats", "--verbose");
        tool.assertOnCleanExit();
        String stdout = tool.getStdout();
        /*
         * Example expected output:
         * Address          SSL   Cipher                 Protocol  Version User      Keyspace Requests Driver-Name          Driver-Version Client-Options                                                             Auth-Mode Auth-Metadata
         * /127.0.0.1:57141 false undefined              undefined 5       cassandra          17       DataStax Java Driver 3.11.5         DRIVER_VERSION=3.11.5, DRIVER_NAME=DataStax Java Driver, CQL_VERSION=3.0.0 Password
         * /127.0.0.1:57165 true  TLS_AES_256_GCM_SHA384 TLSv1.3   5       cassandra system   3        DataStax Java Driver 3.11.5         DRIVER_VERSION=3.11.5, DRIVER_NAME=DataStax Java Driver, CQL_VERSION=3.0.0 MutualTls identity=spiffe://test.cassandra.apache.org/unitTest/mtls
         * /127.0.0.1:57164 true  TLS_AES_256_GCM_SHA384 TLSv1.3   5       cassandra          3        DataStax Java Driver 3.11.5         DRIVER_VERSION=3.11.5, DRIVER_NAME=DataStax Java Driver, CQL_VERSION=3.0.0 Password
         * /127.0.0.1:57144 true  TLS_AES_256_GCM_SHA384 TLSv1.3   5       cassandra          17       DataStax Java Driver 3.11.5         DRIVER_VERSION=3.11.5, DRIVER_NAME=DataStax Java Driver, CQL_VERSION=3.0.0 Password
         * /127.0.0.1:57146 true  TLS_AES_256_GCM_SHA384 TLSv1.3   5       cassandra          16       DataStax Java Driver 3.11.5         DRIVER_VERSION=3.11.5, DRIVER_NAME=DataStax Java Driver, CQL_VERSION=3.0.0 MutualTls identity=spiffe://test.cassandra.apache.org/unitTest/mtls
         * /127.0.0.1:57163 false undefined              undefined 5       cassandra          4        DataStax Java Driver 3.11.5         DRIVER_VERSION=3.11.5, DRIVER_NAME=DataStax Java Driver, CQL_VERSION=3.0.0 Password
         */
        // Header
        assertThat(stdout).containsPattern("Address +SSL +Cipher +Protocol +Version +User +Keyspace +Requests +Driver-Name +Driver-Version +Client-Options +Auth-Mode +Auth-Metadata");
        // Unencrypted password-based client. Expect 'DRIVER_VERSION' to appear before Password.
        assertThat(stdout).containsPattern("/127.0.0.1:[0-9]+ false +undefined +undefined +[0-9]+ +cassandra +[0-9]+ +DataStax Java Driver 3.11.5 +.*DRIVER_VERSION.* +Password");
        // TLS-encrypted password-based client.
        assertThat(stdout).containsPattern("/127.0.0.1:[0-9]+ true +TLS\\S+ +TLS\\S+ +[0-9]+ +cassandra +[0-9]+ +DataStax Java Driver 3.11.5 +.*DRIVER_VERSION.* +Password");
        // MTLS-based client.
        assertThat(stdout).containsPattern("/127.0.0.1:[0-9]+ true +TLS\\S+ +TLS\\S+ +[0-9]+ +cassandra +[0-9]+ +DataStax Java Driver 3.11.5 +.*DRIVER_VERSION.* +MutualTls +identity=" + TlsTestUtils.CLIENT_SPIFFE_IDENTITY);
        // MTLS-based client with 'system' keyspace set on connection.
        assertThat(stdout).containsPattern("/127.0.0.1:[0-9]+ true +TLS\\S+ +TLS\\S+ +[0-9]+ +cassandra +system +[0-9]+ +DataStax Java Driver 3.11.5 +.*DRIVER_VERSION.* +MutualTls +identity=" + TlsTestUtils.CLIENT_SPIFFE_IDENTITY);

        assertClientCount(stdout);
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

    public void assertClientCount(String stdout)
    {
        // Expect two connections for each client (1 control connection, 1 core pool connection)
        assertThat(stdout).contains("Total connected clients: 6");
        assertThat(stdout).contains("User      Connections");
        assertThat(stdout).contains("cassandra 6");
    }
}
