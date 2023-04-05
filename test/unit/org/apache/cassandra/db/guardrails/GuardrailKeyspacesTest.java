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

package org.apache.cassandra.db.guardrails;

import org.junit.Test;

import org.apache.cassandra.schema.Schema;

import static java.lang.String.format;

/**
 * Tests the guardrail for the max number of user keyspaces, {@link Guardrails#keyspaces}.
 */
public class GuardrailKeyspacesTest extends ThresholdTester
{
    private static final int WARN_THRESHOLD = 3; // CQLTester creates two keyspaces
    private static final int FAIL_THRESHOLD = WARN_THRESHOLD + 1;

    public GuardrailKeyspacesTest()
    {
        super(WARN_THRESHOLD,
              FAIL_THRESHOLD,
              Guardrails.keyspaces,
              Guardrails::setKeyspacesThreshold,
              Guardrails::getKeyspacesWarnThreshold,
              Guardrails::getKeyspacesFailThreshold);
    }

    @Override
    protected long currentValue()
    {
        return Schema.instance.getUserKeyspaces().size();
    }

    @Test
    public void testCreateKeyspace() throws Throwable
    {
        // create keyspaces until hitting the two warn/fail thresholds
        String k1 = assertCreateKeyspaceValid();
        String k2 = assertCreateKeyspaceWarns();
        assertCreateKeyspaceFails();

        // drop a keyspace and hit the warn/fail threshold again
        dropKeyspace(k2);
        String k3 = assertCreateKeyspaceWarns();
        assertCreateKeyspaceFails();

        // drop two keyspaces and hit the warn/fail threshold again
        dropKeyspace(k1);
        dropKeyspace(k3);
        assertCreateKeyspaceValid();
        assertCreateKeyspaceWarns();
        assertCreateKeyspaceFails();

        // test excluded users
        testExcludedUsers(this::createKeyspaceQuery,
                          this::createKeyspaceQuery,
                          this::createKeyspaceQuery);
    }

    private void dropKeyspace(String keyspaceName)
    {
        schemaChange(format("DROP KEYSPACE %s", keyspaceName));
    }

    private String assertCreateKeyspaceValid() throws Throwable
    {
        String keyspaceName = createKeyspaceName();
        assertMaxThresholdValid(createKeyspaceQuery(keyspaceName));
        return keyspaceName;
    }

    private String assertCreateKeyspaceWarns() throws Throwable
    {
        String keyspaceName = createKeyspaceName();
        assertThresholdWarns(createKeyspaceQuery(keyspaceName),
                             format("Creating keyspace %s, current number of keyspaces %d exceeds warning threshold of %d",
                                    keyspaceName, currentValue() + 1, WARN_THRESHOLD)
        );
        return keyspaceName;
    }

    private void assertCreateKeyspaceFails() throws Throwable
    {
        String keyspaceName = createKeyspaceName();
        assertThresholdFails(createKeyspaceQuery(keyspaceName),
                             format("Cannot have more than %d keyspaces, aborting the creation of keyspace %s",
                                    FAIL_THRESHOLD, keyspaceName)
        );
    }

    private String createKeyspaceQuery()
    {
        return createKeyspaceQuery(createKeyspaceName());
    }

    private String createKeyspaceQuery(String keyspaceName)
    {
        return format("CREATE KEYSPACE %s WITH replication={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }",
                      keyspaceName);
    }
}
