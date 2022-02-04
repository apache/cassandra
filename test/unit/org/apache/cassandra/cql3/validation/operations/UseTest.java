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
package org.apache.cassandra.cql3.validation.operations;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.exceptions.InvalidRequestException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class UseTest extends CQLTester
{
    @Test
    public void testUseStatementWithBindVariable() throws Throwable
    {
        DatabaseDescriptor.setUseStatementsEnabled(true);
        assertInvalidSyntaxMessage("Bind variables cannot be used for keyspace names", "USE ?");
    }

    @Test
    public void shouldRejectUseStatementWhenProhibited() throws Throwable
    {
        long useCountBefore = QueryProcessor.metrics.useStatementsExecuted.getCount();

        try
        {
            DatabaseDescriptor.setUseStatementsEnabled(false);
            execute("USE cql_test_keyspace");
            fail("expected USE statement to fail with use_statements_enabled = false");
        }
        catch (InvalidRequestException e)
        {
            assertEquals(useCountBefore, QueryProcessor.metrics.useStatementsExecuted.getCount());
            Assertions.assertThat(e).hasMessageContaining("USE statements prohibited");
        }
    }
}
