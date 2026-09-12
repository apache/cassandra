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

package org.apache.cassandra.cql3.statements.schema;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.schema.SchemaTransformation;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.membership.NodeAddresses;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.tcm.transformations.AlterSchema;
import org.apache.cassandra.tcm.transformations.Startup;
import org.apache.cassandra.utils.CassandraVersion;

import static org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper.addr;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * DDL carrying {@code flush_compression} is rejected while the cluster common serialization version is below V11.
 */
public class FlushCompressionVersionCompatibilityTest
{
    private static final String KS = "fc_ks";

    private NodeId oldNode;
    private NodeAddresses oldNodeAddresses;

    @BeforeClass
    public static void beforeClass()
    {
        ServerTestUtils.prepareServerNoRegister();
        DatabaseDescriptor.setMaterializedViewsEnabled(true);
        // ClusterMetadataTestHelper.join assigns Murmur3 tokens
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    @Before
    public void before()
    {
        ClusterMetadataService.unsetInstance();
        ClusterMetadataTestHelper.setInstanceForTest();

        for (int i = 1; i <= 4; i++)
        {
            NodeId nodeId = ClusterMetadataTestHelper.register(addr(i), "dc0", "rack0",
                                                               new NodeVersion(CassandraVersion.CASSANDRA_5_1, i == 1 ? Version.V10 : Version.V11));
            if (i == 1)
            {
                oldNode = nodeId;
                oldNodeAddresses = new NodeAddresses(addr(i));
            }
            ClusterMetadataTestHelper.join(i, i);
        }

        commit("CREATE KEYSPACE " + KS + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        commit("CREATE TABLE " + KS + ".base (k int PRIMARY KEY, v int)");
        commit("CREATE MATERIALIZED VIEW " + KS + ".mv AS SELECT k, v FROM " + KS + ".base WHERE k IS NOT NULL AND v IS NOT NULL PRIMARY KEY (v, k)");
    }

    @Test
    public void minimumSerializationVersion()
    {
        TableAttributes without = new TableAttributes();
        without.addProperty("comment", "x");
        assertThat(without.minimumSerializationVersion()).isEqualTo(Version.V0);

        TableAttributes with = new TableAttributes();
        with.addProperty("flush_compression", "none");
        assertThat(with.minimumSerializationVersion()).isEqualTo(Version.V11);
    }

    @Test
    public void createTable()
    {
        assertRejected("CREATE TABLE " + KS + ".t1 (k int PRIMARY KEY, v int) WITH flush_compression = 'none'");
        commit("CREATE TABLE " + KS + ".t1 (k int PRIMARY KEY, v int) WITH comment = 'no flush option'");
        upgradeOldNode();
        commit("CREATE TABLE " + KS + ".t2 (k int PRIMARY KEY, v int) WITH flush_compression = 'none'");
    }

    @Test
    public void alterTable()
    {
        assertRejected("ALTER TABLE " + KS + ".base WITH flush_compression = 'table'");
        commit("ALTER TABLE " + KS + ".base WITH comment = 'no flush option'");
        upgradeOldNode();
        commit("ALTER TABLE " + KS + ".base WITH flush_compression = 'table'");
    }

    @Test
    public void createTableLike()
    {
        assertRejected("CREATE TABLE " + KS + ".copy1 LIKE " + KS + ".base WITH flush_compression = 'fast'");
        commit("CREATE TABLE " + KS + ".copy1 LIKE " + KS + ".base");
        upgradeOldNode();
        commit("CREATE TABLE " + KS + ".copy2 LIKE " + KS + ".base WITH flush_compression = 'fast'");
    }

    @Test
    public void createView()
    {
        String select = " AS SELECT k, v FROM " + KS + ".base WHERE k IS NOT NULL AND v IS NOT NULL PRIMARY KEY (v, k)";
        assertRejected("CREATE MATERIALIZED VIEW " + KS + ".mv1" + select + " WITH flush_compression = 'none'");
        commit("CREATE MATERIALIZED VIEW " + KS + ".mv1" + select);
        upgradeOldNode();
        commit("CREATE MATERIALIZED VIEW " + KS + ".mv2" + select + " WITH flush_compression = 'none'");
    }

    @Test
    public void alterView()
    {
        assertRejected("ALTER MATERIALIZED VIEW " + KS + ".mv WITH flush_compression = 'none'");
        commit("ALTER MATERIALIZED VIEW " + KS + ".mv WITH comment = 'no flush option'");
        upgradeOldNode();
        commit("ALTER MATERIALIZED VIEW " + KS + ".mv WITH flush_compression = 'none'");
    }

    private void upgradeOldNode()
    {
        ClusterMetadataService.instance().commit(new Startup(oldNode, oldNodeAddresses, new NodeVersion(CassandraVersion.CASSANDRA_5_1, Version.V11)));
    }

    private static void commit(String cql)
    {
        ClusterMetadataService.instance().commit(new AlterSchema(transformation(cql)));
    }

    private static void assertRejected(String cql)
    {
        assertThatThrownBy(() -> commit(cql))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Transformation rejected");
    }

    private static SchemaTransformation transformation(String cql)
    {
        return (SchemaTransformation) QueryProcessor.parseStatement(cql, ClientState.forInternalCalls());
    }
}
