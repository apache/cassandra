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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.QueryResults;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.distributed.shared.AssertUtils;
import org.apache.cassandra.distributed.shared.NetworkTopology;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.EACH_QUORUM;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.LOCAL_ONE;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.LOCAL_QUORUM;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ONE;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.QUORUM;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.THREE;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.TWO;

@SuppressWarnings("unchecked")
public class TransientReplicaBasicTest extends TestBaseImpl
{

    @Test
    public void testTokenReadSimpleStrategy() throws IOException
    {
        try (Cluster cluster = init(Cluster.build(3)
                                           .withConfig(conf -> conf.set("transient_replication_enabled", "true")
                                                                   .set("num_tokens", 1)
                                                                   .with(Feature.NETWORK, Feature.GOSSIP))
                                           .start()))
        {
            cluster.schemaChange("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy','replication_factor': " + getRandomRF() + "}");
            cluster.schemaChange("CREATE TABLE ks.users (\n" +
                                 "    user_id varchar PRIMARY KEY,\n" +
                                 "    first varchar,\n" +
                                 "    last varchar,\n" +
                                 "    age int\n" +
                                 ") WITH read_repair = 'NONE';");
            String insert = "INSERT INTO ks.users (user_id, first, last, age) VALUES ('foo', 'foo', 'foo', 18);";
            cluster.coordinator(1).executeWithResult(insert, ConsistencyLevel.ALL);

            List<ConsistencyLevel> cls = Arrays.asList(ONE, TWO, THREE, QUORUM, ALL, LOCAL_QUORUM, LOCAL_ONE);
            for (ConsistencyLevel level : cls)
            {
                assertTokenReadWithConsistency(cluster, level);
            }
        }
    }

    @Test
    public void testTokenReadNetworkTopology() throws IOException
    {
        try (Cluster cluster = init(Cluster.build(6)
                                           .withNodeIdTopology(NetworkTopology.networkTopology(6, (i) ->
                                                                                                  NetworkTopology.dcAndRack("dc" + (i <= 3 ? 0 : 1), "rack" + i)))
                                           .withConfig(conf -> conf.set("transient_replication_enabled", "true")
                                                                   .set("num_tokens", 1)
                                                                   .with(Feature.NETWORK, Feature.GOSSIP))
                                           .start()))
        {

            cluster.schemaChange("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy',\n" +
                                 "'dc0' : '3/1', 'dc1' : " + getRandomRF() +"}");

            cluster.schemaChange("CREATE TABLE ks.users (\n" +
                                 "    user_id varchar PRIMARY KEY,\n" +
                                 "    first varchar,\n" +
                                 "    last varchar,\n" +
                                 "    age int\n" +
                                 ") WITH read_repair = 'NONE';");

            String insert = "INSERT INTO ks.users (user_id, first, last, age) VALUES ('foo', 'foo', 'foo', 18);";
            cluster.coordinator(1).executeWithResult(insert, ConsistencyLevel.EACH_QUORUM);

            List<ConsistencyLevel> cls = Arrays.asList(ONE, TWO, THREE, QUORUM, LOCAL_QUORUM, LOCAL_ONE, EACH_QUORUM, ALL);
            for (ConsistencyLevel level : cls)
            {
                assertTokenReadWithConsistency(cluster, level);
            }
        }
    }

    @Test
    public void testRangeReadSimpleStrategy() throws IOException
    {
        try (Cluster cluster = init(Cluster.build(3)
                                           .withConfig(conf -> conf.set("transient_replication_enabled", "true")
                                                                   .set("num_tokens", 1)
                                                                   .with(Feature.NETWORK, Feature.GOSSIP))
                                           .start()))
        {
            cluster.schemaChange("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy','replication_factor': " + getRandomRF() + "}");
            cluster.schemaChange("CREATE TABLE ks.users (\n" +
                                 "    user_id varchar PRIMARY KEY,\n" +
                                 "    first varchar,\n" +
                                 "    last varchar,\n" +
                                 "    age int\n" +
                                 ") WITH read_repair = 'NONE';");
            String insert = "INSERT INTO ks.users (user_id, first, last, age) VALUES ('foo', 'foo', 'foo', 18);";
            cluster.coordinator(1).executeWithResult(insert, ConsistencyLevel.ALL);

            // if not ONE/LOCAL_ONE, other Consistency will get exception
            List<ConsistencyLevel> cls = Arrays.asList(ONE, LOCAL_ONE);
            for (ConsistencyLevel level : cls)
            {
                assertRangeReadWithConsistency(cluster, level);
            }
        }
    }

    @Test
    public void testRangeReadNetworkTopology() throws IOException
    {
        try (Cluster cluster = init(Cluster.build(6)
                                           .withNodeIdTopology(NetworkTopology.networkTopology(6, (i) ->
                                                                                                  NetworkTopology.dcAndRack("dc" + (i <= 3 ? 0 : 1), "rack" + i)))
                                           .withConfig(conf -> conf.set("transient_replication_enabled", "true")
                                                                   .set("num_tokens", 1)
                                                                   .with(Feature.NETWORK, Feature.GOSSIP))
                                           .start()))
        {

            cluster.schemaChange("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy',\n" +
                                 "'dc0' : " + getRandomRF() + ", 'dc1' : " + getRandomRF() + "}");

            cluster.schemaChange("CREATE TABLE ks.users (\n" +
                                 "    user_id varchar PRIMARY KEY,\n" +
                                 "    first varchar,\n" +
                                 "    last varchar,\n" +
                                 "    age int\n" +
                                 ") WITH read_repair = 'NONE';");

            String insert = "INSERT INTO ks.users (user_id, first, last, age) VALUES ('foo', 'foo', 'foo', 18);";
            cluster.coordinator(1).executeWithResult(insert, ConsistencyLevel.EACH_QUORUM);

            // if not ONE/LOCAL_ONE, other Consistency will get exception
            List<ConsistencyLevel> cls = Arrays.asList(ONE, LOCAL_ONE);
            for (ConsistencyLevel level : cls)
            {
                assertRangeReadWithConsistency(cluster, level);
            }
        }
    }

    @Test
    public void testRangeReadSimpleStrategyShouldFail() throws IOException
    {
        try (Cluster cluster = init(Cluster.build(3)
                                           .withConfig(conf -> conf.set("transient_replication_enabled", "true")
                                                                   .set("num_tokens", 1)
                                                                   .with(Feature.NETWORK, Feature.GOSSIP))
                                           .start()))
        {
            cluster.schemaChange("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy','replication_factor': " + getRandomRF() + "}");
            cluster.schemaChange("CREATE TABLE ks.users (\n" +
                                 "    user_id varchar PRIMARY KEY,\n" +
                                 "    first varchar,\n" +
                                 "    last varchar,\n" +
                                 "    age int\n" +
                                 ") WITH read_repair = 'NONE';");
            String insert = "INSERT INTO ks.users (user_id, first, last, age) VALUES ('foo', 'foo', 'foo', 18);";
            cluster.coordinator(1).executeWithResult(insert, ConsistencyLevel.ALL);

            List<ConsistencyLevel> cls = Arrays.asList(ONE, TWO, THREE, QUORUM, ALL, LOCAL_QUORUM, LOCAL_ONE);
            for (ConsistencyLevel level : cls)
            {
                assertRangeReadWithGreaterThan(cluster, level);
            }
            Assert.fail("transient replication range read with greater than should fail");
        }
        catch (Exception e)
        {
        }
    }

    @Test
    public void testWriteSimpleStrategy() throws IOException
    {
        try (Cluster cluster = init(Cluster.build(3)
                                           .withConfig(conf -> conf.set("transient_replication_enabled", "true")
                                                                   .set("num_tokens", 1)
                                                                   .with(Feature.NETWORK, Feature.GOSSIP))
                                           .start()))
        {
            cluster.schemaChange("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy','replication_factor': " + getRandomRF() + "}");

            cluster.schemaChange("CREATE TABLE ks.users (\n" +
                                 "    user_id varchar PRIMARY KEY,\n" +
                                 "    first varchar,\n" +
                                 "    last varchar,\n" +
                                 "    age int\n" +
                                 ") WITH read_repair = 'NONE';");

            List<ConsistencyLevel> cls = Arrays.asList(ONE, TWO, THREE, QUORUM, ALL, LOCAL_QUORUM, LOCAL_ONE);
            for (ConsistencyLevel level : cls)
            {
                assertWriteWithConsistency(cluster, level);
            }
        }
    }

    @Test
    public void testWriteNetworkTopology() throws IOException
    {
        try (Cluster cluster = init(Cluster.build(6)
                                           .withNodeIdTopology(NetworkTopology.networkTopology(6, (i) ->
                                                                                                  NetworkTopology.dcAndRack("dc" + (i <= 3 ? 0 : 1), "rack" + i)))
                                           .withConfig(conf -> conf.set("transient_replication_enabled", "true")
                                                                   .set("num_tokens", 1)
                                                                   .with(Feature.NETWORK, Feature.GOSSIP))
                                           .start()))
        {
            cluster.schemaChange("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy',\n" +
                                 "'dc0' : " + getRandomRF() + ", 'dc1' : " + getRandomRF() + "}");

            cluster.schemaChange("CREATE TABLE ks.users (\n" +
                                 "    user_id varchar PRIMARY KEY,\n" +
                                 "    first varchar,\n" +
                                 "    last varchar,\n" +
                                 "    age int\n" +
                                 ") WITH read_repair = 'NONE';");

            List<ConsistencyLevel> cls = Arrays.asList(ONE, TWO, THREE, QUORUM, LOCAL_QUORUM, LOCAL_ONE, EACH_QUORUM, ALL);
            for (ConsistencyLevel level : cls)
            {
                assertWriteWithConsistency(cluster, level);
            }
        }
    }

    private static void assertRangeReadWithConsistency(Cluster cluster, ConsistencyLevel consistencyLevel)
    {
        String query = "select user_id, first, last, age from ks.users";
        SimpleQueryResult result = cluster.coordinator(1).executeWithResult(query, consistencyLevel);

        query = "select user_id, first, last, age from ks.users WHERE token(user_id) <= token('10000000')";
        SimpleQueryResult result2 = cluster.coordinator(1).executeWithResult(query, consistencyLevel);

        query = "select user_id, first, last, age from ks.users WHERE token(user_id) <= token('100')";
        SimpleQueryResult result3 = cluster.coordinator(1).executeWithResult(query, consistencyLevel);

        AssertUtils.assertRows(result, QueryResults.builder().row("foo", "foo", "foo", 18).build());
        AssertUtils.assertRows(result2, QueryResults.builder().row("foo", "foo", "foo", 18).build());
        AssertUtils.assertRows(result3, QueryResults.builder().row("foo", "foo", "foo", 18).build());
    }

    private static void assertRangeReadWithGreaterThan(Cluster cluster, ConsistencyLevel consistencyLevel)
    {
        String query = "select user_id, first, last, age from ks.users";
        cluster.coordinator(1).executeWithResult(query, consistencyLevel);

        query = "select user_id, first, last, age from ks.users WHERE token(user_id) > token('10000000')";
        cluster.coordinator(1).executeWithResult(query, consistencyLevel);

        query = "select user_id, first, last, age from ks.users WHERE token(user_id) > token('100')";
        cluster.coordinator(1).executeWithResult(query, consistencyLevel);
    }

    private static void assertTokenReadWithConsistency(Cluster cluster, ConsistencyLevel consistencyLevel)
    {
        String query = "select user_id, first, last, age from ks.users where user_id = 'foo';";
        SimpleQueryResult result = cluster.coordinator(1).executeWithResult(query, consistencyLevel);
        AssertUtils.assertRows(result, QueryResults.builder().row("foo", "foo", "foo", 18).build());
    }

    private static void assertWriteWithConsistency(Cluster cluster, ConsistencyLevel consistencyLevel)
    {
        String insert = "INSERT INTO ks.users (user_id, first, last, age) VALUES ('" + consistencyLevel.code + "', 'foo', 'foo', " + consistencyLevel.code + ");";
        cluster.coordinator(1).executeWithResult(insert, ALL);

        String query = "select user_id, first, last, age from ks.users where user_id = '" + consistencyLevel.code + "';";
        SimpleQueryResult result = cluster.coordinator(1).executeWithResult(query, consistencyLevel);
        AssertUtils.assertRows(result, QueryResults.builder().row(String.valueOf(consistencyLevel.code), "foo", "foo", consistencyLevel.code).build());
    }

    private static String getRandomRF() {
        return ThreadLocalRandom.current().nextInt(2) == 0 ? "'3/1'" : "'3/2'";
    }
}
