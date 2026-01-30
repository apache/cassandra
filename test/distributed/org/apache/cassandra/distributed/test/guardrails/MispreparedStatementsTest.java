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

import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.exceptions.InvalidQueryException;

import org.assertj.core.api.ThrowableAssert;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.db.guardrails.PreparedStatementParameterRequirementGuardrail;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class MispreparedStatementsTest extends GuardrailTester
{
    private static Cluster cluster;
    private static com.datastax.driver.core.Cluster driverCluster;
    private static Session driverSession;

    @BeforeClass
    public static void setupCluster() throws IOException
    {
        cluster = init(Cluster.build(1)
                              .withConfig(c -> c.with(Feature.GOSSIP, Feature.NATIVE_PROTOCOL)
                                                .set("authenticator", "PasswordAuthenticator")
                                                .set("prepared_statements_require_parameters_warned", true)
                                                .set("prepared_statements_require_parameters_enabled", false))
                              .start());
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

    @Test
    public void testInvalidConstantSelectStatements()
    {
        schemaChange("create table %s (pk int, ck int, data text, primary key((pk), ck))");

        cluster.get(1).runOnInstance(() -> {
            Guardrails.instance.setPreparedStatementsRequireParametersEnabled(true);
        });
        assertGuardrailViolated(() -> prepare("select * from %s where pk = 1"));
        assertGuardrailViolated(() -> prepare("select * from %s where ck = 1 allow filtering"));
        assertGuardrailViolated(() -> prepare("select * from %s where data = 'a' allow filtering"));
        assertGuardrailViolated(() -> prepare("insert into %s(pk, ck, data) values (1,1,'a')"));
        assertGuardrailViolated(() -> prepare("update %s set data = 'a' where pk = 1 and ck = 1"));

        // non-prepare queries remain unaffected
        assertThatCode(() -> execute("select * from %s where pk = 1")).doesNotThrowAnyException();
        // at-least 1 bind marker
        assertThatCode(() -> prepare("select * from %s where pk = 1 AND data = ? allow filtering")).doesNotThrowAnyException();
        // statement having bind marker but no where clause
        assertThatCode(() -> prepare("insert into %s(pk, ck, data) values (1,1,?)")).doesNotThrowAnyException();
    }

    private void assertGuardrailViolated(ThrowableAssert.ThrowingCallable throwable)
    {
        assertThatThrownBy(throwable)
        .isInstanceOf(InvalidQueryException.class)
        .hasMessageContaining(PreparedStatementParameterRequirementGuardrail.MISPREPARED_STATEMENT_MESSAGE);
    }

    @Override
    protected Session getSession()
    {
        return driverSession;
    }

    private void execute(String query)
    {
        SimpleStatement stmt = new SimpleStatement(format(query));
        stmt.setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.ALL);
        driverSession.execute(stmt);
    }

    private void prepare(String query)
    {
        SimpleStatement stmt = new SimpleStatement(format(query));
        stmt.setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.ALL);
        driverSession.prepare(stmt);
    }
}
