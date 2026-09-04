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

package org.apache.cassandra.cql3.terms;

import java.nio.ByteBuffer;
import java.util.Collections;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.service.QueryState;

import static org.junit.Assert.fail;

public class InMarkerTest extends CQLTester
{
    @Test
    public void testNotEnoughBytesThrowsInvalidRequest() throws Throwable
    {
        assertInMarkerRejectsMalformedValue(new byte[]{ 0, 0, 0, 1 });
    }

    @Test
    public void testExtraneousBytesThrowsInvalidRequest() throws Throwable
    {
        assertInMarkerRejectsMalformedValue(new byte[]{ 0, 0, 0, 0, 9, 9 });
    }

    private void assertInMarkerRejectsMalformedValue(byte[] malformedListBytes) throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v int)");
        SelectStatement select = (SelectStatement) parseStatement("SELECT * FROM " + KEYSPACE + '.' + currentTable() + " WHERE pk IN ?");

        QueryOptions options = QueryOptions.forInternalCalls(Collections.singletonList(ByteBuffer.wrap(malformedListBytes)));
        try
        {
            select.getQuery(options, QueryState.forInternalCalls().getNowInSeconds());
            fail("Expected InvalidRequestException to be thrown for a malformed IN marker value");
        }
        catch (InvalidRequestException e)
        {
            // expected
        }
    }
}
