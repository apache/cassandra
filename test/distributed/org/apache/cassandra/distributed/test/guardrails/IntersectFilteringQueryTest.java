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

package org.apache.cassandra.distributed.test.guardrails;

import java.io.IOException;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.distributed.test.sai.SAIUtil;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

public class IntersectFilteringQueryTest extends GuardrailTester
{
    private static Cluster cluster;
    private static com.datastax.driver.core.Cluster driverCluster;
    private static Session driverSession;

    @BeforeClass
    public static void setupCluster() throws IOException
    {
        cluster = init(Cluster.build(2).withConfig(c -> c.with(GOSSIP, NATIVE_PROTOCOL, NETWORK)
                                                         .set("read_thresholds_enabled", "true")
                                                         .set("authenticator", "PasswordAuthenticator")).start());

        driverCluster = buildDriverCluster(cluster);
        driverSession = driverCluster.connect();
    }

    @AfterClass
    public static void teardownCluster()
    {
        if (driverSession != null)
            driverSession.close();

        if (driverCluster != null)
            driverCluster.close();

        if (cluster != null)
            cluster.close();
    }

    @Override
    protected Cluster getCluster()
    {
        return cluster;
    }

    @Override
    protected Session getSession()
    {
        return driverSession;
    }

    @Test
    @SuppressWarnings({"DataFlowIssue", "ResultOfMethodCallIgnored"})
    public void shouldWarnOnFilteringQuery()
    {
        cluster.forEach(instance -> instance.runOnInstance((IIsolatedExecutor.SerializableRunnable) () -> Guardrails.instance.setIntersectFilteringQueryWarned(true)));
        cluster.forEach(instance -> instance.runOnInstance((IIsolatedExecutor.SerializableRunnable) () -> Guardrails.instance.setIntersectFilteringQueryEnabled(true)));
        
        schemaChange("CREATE TABLE IF NOT EXISTS %s (k bigint, c bigint, v1 bigint, v2 bigint, PRIMARY KEY (k, c))");
        List<String> globalWarnings = executeViaDriver(format("SELECT * FROM %s WHERE v1 = 0 AND v2 = 0 ALLOW FILTERING"));
        assertThat(globalWarnings).satisfiesOnlyOnce(s -> s.contains(Guardrails.intersectFilteringQueryEnabled.reason));
        List<String> partitionWarnings = executeViaDriver(format("SELECT * FROM %s WHERE k = 0 AND v1 = 0 AND v2 = 0 ALLOW FILTERING"));
        assertThat(partitionWarnings).satisfiesOnlyOnce(s -> s.contains(Guardrails.intersectFilteringQueryEnabled.reason));
    }

    @Test
    public void shouldFailOnFilteringQuery()
    {
        cluster.forEach(instance -> instance.runOnInstance((IIsolatedExecutor.SerializableRunnable) () -> Guardrails.instance.setIntersectFilteringQueryEnabled(false)));

        schemaChange("CREATE TABLE IF NOT EXISTS %s (k bigint, c bigint, v1 bigint, v2 bigint, PRIMARY KEY (k, c))");

        assertThatThrownBy(() -> executeViaDriver(format("SELECT * FROM %s WHERE k = 0 AND v1 = 0 AND v2 = 0 ALLOW FILTERING")))
                .isInstanceOf(InvalidQueryException.class)
                .hasMessageContaining(Guardrails.intersectFilteringQueryEnabled.reason);
    }

    @Test
    public void shouldNotWarnOrFailOnIndexQuery()
    {
        cluster.forEach(instance -> instance.runOnInstance((IIsolatedExecutor.SerializableRunnable) () -> Guardrails.instance.setIntersectFilteringQueryWarned(true)));
        cluster.forEach(instance -> instance.runOnInstance((IIsolatedExecutor.SerializableRunnable) () -> Guardrails.instance.setIntersectFilteringQueryEnabled(false)));
        
        schemaChange("CREATE TABLE %s (k bigint, c bigint, v1 bigint, v2 bigint, PRIMARY KEY (k, c))");
        schemaChange("CREATE INDEX ON %s(v1) USING 'sai'");
        schemaChange("CREATE INDEX ON %s(v2) USING 'sai'");
        SAIUtil.waitForIndexQueryable(getCluster(), KEYSPACE);

        List<String> globalWarnings = executeViaDriver(format("SELECT * FROM %s WHERE v1 = 0 AND v2 = 0"));
        assertThat(globalWarnings).isEmpty();
        List<String> partitionWarnings = executeViaDriver(format("SELECT * FROM %s WHERE k = 0 AND v1 = 0 AND v2 = 0"));
        assertThat(partitionWarnings).isEmpty();
    }
}
