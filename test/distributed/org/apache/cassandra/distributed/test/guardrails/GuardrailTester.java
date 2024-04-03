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

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import org.apache.cassandra.distributed.util.Auth;
import org.junit.After;
import org.junit.Before;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.ListAssert;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class GuardrailTester extends TestBaseImpl
{
    private static final AtomicInteger seqNumber = new AtomicInteger();
    protected String tableName, qualifiedTableName;

    protected abstract Cluster getCluster();

    protected Session getSession()
    {
        return null;
    }

    @Before
    public void beforeTest()
    {
        tableName = "t_" + seqNumber.getAndIncrement();
        qualifiedTableName = KEYSPACE + '.' + tableName;
    }

    @After
    public void afterTest()
    {
        schemaChange("DROP TABLE IF EXISTS %s");
    }

    protected static com.datastax.driver.core.Cluster buildDriverCluster(Cluster cluster)
    {
        Auth.waitForExistingRoles(cluster.get(1));

        // create a regular user, since the default superuser is excluded from guardrails
        com.datastax.driver.core.Cluster.Builder builder = com.datastax.driver.core.Cluster.builder().addContactPoint("127.0.0.1");
        try (com.datastax.driver.core.Cluster c = builder.withCredentials("cassandra", "cassandra").build();
             Session session = c.connect())
        {
            session.execute("CREATE USER test WITH PASSWORD 'test'");
        }

        // connect using that superuser, we use the driver to get access to the client warnings
        return builder.withCredentials("test", "test").build();
    }

    /**
     * Execution of statements via driver will not bypass guardrails as internal queries would do as they are
     * done by superuser / they do not have any notion of roles
     *
     * @return list of warnings
     */
    protected List<String> executeViaDriver(String query)
    {
        SimpleStatement stmt = new SimpleStatement(query);
        stmt.setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.QUORUM);
        ResultSet resultSet = getSession().execute(stmt);
        return resultSet.getExecutionInfo().getWarnings();
    }

    protected String format(String query)
    {
        return String.format(query, qualifiedTableName);
    }

    protected void schemaChange(String query)
    {
        getCluster().schemaChange(format(query));
    }

    protected void assertNotWarnedOnFlush()
    {
        assertNotWarnsOnSSTableWrite(false);
    }

    protected void assertNotWarnedOnCompact()
    {
        assertNotWarnsOnSSTableWrite(true);
    }

    protected void assertNotWarnsOnSSTableWrite(boolean compact)
    {
        getCluster().stream().forEach(node -> assertNotWarnsOnSSTableWrite(node, compact));
    }

    protected void assertNotWarnsOnSSTableWrite(IInstance node, boolean compact)
    {
        long mark = node.logs().mark();
        try
        {
            writeSSTables(node, compact);
            assertTrue(node.logs().grep(mark, "^ERROR", "^WARN").getResult().isEmpty());
        }
        catch (InvalidRequestException e)
        {
            fail("Expected not to fail, but Fails with error message: " + e.getMessage());
        }
    }

    protected void assertWarnedOnFlush(String... msgs)
    {
        assertWarnsOnSSTableWrite(false, msgs);
    }

    protected void assertWarnedOnCompact(String... msgs)
    {
        assertWarnsOnSSTableWrite(true, msgs);
    }

    protected void assertWarnsOnSSTableWrite(boolean compact, String... msgs)
    {
        getCluster().stream().forEach(node -> assertWarnsOnSSTableWrite(node, compact, msgs));
    }

    protected void assertWarnsOnSSTableWrite(IInstance node, boolean compact, String... msgs)
    {
        long mark = node.logs().mark();
        writeSSTables(node, compact);
        assertTrue(node.logs().grep(mark, "^ERROR").getResult().isEmpty());
        List<String> warnings = node.logs().grep(mark, "^WARN").getResult();
        ListAssert<String> assertion = Assertions.assertThat(warnings).isNotEmpty().hasSize(msgs.length);
        for (String msg : msgs)
            assertion.anyMatch(m -> m.contains(msg));
    }

    protected void assertFailedOnFlush(String... msgs)
    {
        assertFailsOnSSTableWrite(false, msgs);
    }

    protected void assertFailedOnCompact(String... msgs)
    {
        assertFailsOnSSTableWrite(true, msgs);
    }

    private void assertFailsOnSSTableWrite(boolean compact, String... msgs)
    {
        getCluster().stream().forEach(node -> assertFailsOnSSTableWrite(node, compact, msgs));
    }

    private void assertFailsOnSSTableWrite(IInstance node, boolean compact, String... msgs)
    {
        long mark = node.logs().mark();
        writeSSTables(node, compact);
        assertTrue(node.logs().grep(mark, "^WARN").getResult().isEmpty());
        List<String> warnings = node.logs().grep(mark, "^ERROR").getResult();
        ListAssert<String> assertion = Assertions.assertThat(warnings).isNotEmpty().hasSize(msgs.length);
        for (String msg : msgs)
            assertion.anyMatch(m -> m.contains(msg));
    }

    private void writeSSTables(IInstance node, boolean compact)
    {
        node.flush(KEYSPACE);
        if (compact)
            node.forceCompact(KEYSPACE, tableName);
    }
}
