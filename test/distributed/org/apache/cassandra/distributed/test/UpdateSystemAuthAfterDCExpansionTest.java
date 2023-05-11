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

import java.util.Collections;
import java.util.UUID;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.utils.concurrent.Condition;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.RoleOptions;
import org.apache.cassandra.auth.RoleResource;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.progress.ProgressEventType;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.cassandra.auth.AuthKeyspace.ROLES;
import static org.apache.cassandra.config.CassandraRelevantProperties.SUPERUSER_SETUP_DELAY_MS;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.apache.cassandra.distributed.shared.NetworkTopology.dcAndRack;
import static org.apache.cassandra.distributed.shared.NetworkTopology.networkTopology;
import static org.apache.cassandra.utils.concurrent.Condition.newOneTimeCondition;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/*
 * Test that system_auth can only be altered to have valid datacenters, and that
 * all valid datacenters must have at least one replica.
 *
 * Create a cluster with one nodes in dc1 with a new role
 * Alter the system_auth keyspace to use NTS with {dc1: 1}
 * Expand a cluster with a new node in dc2
 * Alter the system auth keyspace to use NTS with {dc1: 1}, {dc2, 1} & repair
 * Check that the new role is present in the new datacenter
 * Remove the dc2 node
 * Check the keyspace can be altered again to remove it
 */
public class UpdateSystemAuthAfterDCExpansionTest extends TestBaseImpl
{
    static final Logger logger = LoggerFactory.getLogger(UpdateSystemAuthAfterDCExpansionTest.class);
    static final String username = "shinynewuser";

    static void assertRolePresent(IInstance instance)
    {
        assertRows(instance.executeInternal(String.format("SELECT role FROM %s.%s WHERE role = ?",
                                                          SchemaConstants.AUTH_KEYSPACE_NAME, ROLES),
                                            username),
                                            row(username));
    }

    static void assertRoleAbsent(IInstance instance)
    {
        assertRows(instance.executeInternal(String.format("SELECT role FROM %s.%s WHERE role = ?",
                                                          SchemaConstants.AUTH_KEYSPACE_NAME, ROLES),
                                            username));
    }

    static void assertQueryThrowsConfigurationException(Cluster cluster, String query)
    {
        cluster.forEach(instance -> {
            try
            {
                // No need to use cluster.schemaChange as we're expecting a failure
                instance.executeInternal(query);
                fail("Expected \"" + query + "\" to throw a ConfigurationException, but it completed");
            }
            catch (Throwable tr)
            {
                assertEquals("org.apache.cassandra.exceptions.ConfigurationException", tr.getClass().getCanonicalName());
            }
        });
    }

    String alterKeyspaceStatement(String ntsOptions)
    {
        return String.format("ALTER KEYSPACE " + SchemaConstants.AUTH_KEYSPACE_NAME +
                             " WITH replication = {'class': 'NetworkTopologyStrategy', %s};", ntsOptions);
    }

    @BeforeClass
    static public void beforeClass() throws Throwable
    {
        // reduce the time from 10s to prevent "Cannot process role related query as the role manager isn't yet setup."
        // exception from CassandraRoleManager
        SUPERUSER_SETUP_DELAY_MS.setLong(0);
        TestBaseImpl.beforeClass();
    }

    public void validateExpandAndContract(String initialDatacenters,
                                          String expandedDatacenters,
                                          String beforeDecommissionedDatacenters,
                                          String afterDecommissionedDatacenters) throws Throwable
    {
        try (Cluster cluster = Cluster.build(1)
                                      .withConfig(config -> config.set("auto_bootstrap", true)
                                                                      .with(GOSSIP)
                                                                      .with(NETWORK))
                                      .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(2))
                                      .withNodeIdTopology(networkTopology(2,
                                                                          (nodeid) -> nodeid % 2 == 1 ? dcAndRack("dc1", "rack1")
                                                                                                      : dcAndRack("dc2", "rack2")
                                      ))
                                      .withNodes(1)
                                      .createWithoutStarting())
        {
            logger.debug("Starting cluster with single node in dc1");
            cluster.startup();

            // currently no way to set authenticated user for coordinator
            logger.debug("Creating test role");
            cluster.get(1).runOnInstance(() -> DatabaseDescriptor.getRoleManager().createRole(AuthenticatedUser.SYSTEM_USER,
                                                                                              RoleResource.role(username),
                                                                                              new RoleOptions()));
            assertRolePresent(cluster.get(1));

            logger.debug("Try changing NTS too early before a node from the DC has joined");
            assertQueryThrowsConfigurationException(cluster, alterKeyspaceStatement("'dc1': '1', 'dc2': '1'"));

            logger.debug("Altering '{}' keyspace to use NTS with {}", SchemaConstants.AUTH_KEYSPACE_NAME, initialDatacenters);
            cluster.schemaChangeIgnoringStoppedInstances(alterKeyspaceStatement(initialDatacenters));

            logger.debug("Bootstrapping second node in dc2");
            IInstanceConfig config = cluster.newInstanceConfig();
            config.set("auto_bootstrap", true);
            cluster.bootstrap(config).startup();

            // Check that the role is on node1 but has not made it to node2
            assertRolePresent(cluster.get(1));
            assertRoleAbsent(cluster.get(2));

            // Update options to make sure a replica is in the remote DC
            logger.debug("Altering '{}' keyspace to use NTS with dc1 & dc2", SchemaConstants.AUTH_KEYSPACE_NAME);
            cluster.schemaChangeIgnoringStoppedInstances(alterKeyspaceStatement(expandedDatacenters));

            // make sure that all sstables have moved to repaired by triggering a compaction
            logger.debug("Repair system_auth to make sure role is replicated everywhere");
            cluster.get(1).runOnInstance(() -> {
                try
                {
                    Condition await = newOneTimeCondition();
                    StorageService.instance.repair(SchemaConstants.AUTH_KEYSPACE_NAME, Collections.emptyMap(), ImmutableList.of((tag, event) -> {
                        if (event.getType() == ProgressEventType.COMPLETE)
                            await.signalAll();
                    })).right.get();
                    await.await(1L, MINUTES);
                }
                catch (Exception e)
                {
                    fail("Unexpected exception: " + e);
                }
            });

            logger.debug("Check the role is now replicated as expected after repairing");
            assertRolePresent(cluster.get(1));
            assertRolePresent(cluster.get(2));

            // Make sure we cannot remove either of the active datacenters
            logger.debug("Verify that neither active datacenter can be ALTER KEYSPACEd away");
            assertQueryThrowsConfigurationException(cluster, alterKeyspaceStatement("'dc1': '1'"));
            assertQueryThrowsConfigurationException(cluster, alterKeyspaceStatement("'dc2': '1'"));

            logger.debug("Starting to decomission dc2");
            cluster.schemaChangeIgnoringStoppedInstances(alterKeyspaceStatement(beforeDecommissionedDatacenters));

            // Forcibly shutdown and have node2 evicted by FD
            logger.debug("Force shutdown node2");
            String node2hostId = cluster.get(2).callOnInstance(() -> StorageService.instance.getLocalHostId());
            cluster.get(2).shutdown(false);

            logger.debug("removeNode node2");
            cluster.get(1).runOnInstance(() -> {
                UUID hostId = UUID.fromString(node2hostId);
                InetAddressAndPort endpoint = StorageService.instance.getEndpointForHostId(hostId);
                FailureDetector.instance.forceConviction(endpoint);
                StorageService.instance.removeNode(node2hostId);
            });

            logger.debug("Remove replication to decomissioned dc2");
            cluster.schemaChangeIgnoringStoppedInstances(alterKeyspaceStatement(afterDecommissionedDatacenters));
        }
    }

    @Test
    public void explicitDCTest() throws Throwable
    {
        String initialDatacenters = "'dc1': '1'";
        String expandedDatacenters = "'dc1': '1', 'dc2': '1'";
        String beforeDecommissionedDatacenters = "'dc1': '1', 'dc2': '1'";
        String afterDecommissionedDatacenters = "'dc1': '1'";
        validateExpandAndContract(initialDatacenters, expandedDatacenters, beforeDecommissionedDatacenters, afterDecommissionedDatacenters);
    }

    @Test
    public void replicaFactorTest() throws Throwable
    {
        String initialDatacenters = "'replication_factor': '1'";
        String expandedDatacenters = "'replication_factor': '1'";
        String beforeDecommissionedDatacenters = "'replication_factor': '1', 'dc2': '1'";
        String afterDecommissionedDatacenters =  "'dc1': '1'";
        validateExpandAndContract(initialDatacenters, expandedDatacenters, beforeDecommissionedDatacenters, afterDecommissionedDatacenters);
    }
}