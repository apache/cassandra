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

package org.apache.cassandra.cql3;

import java.util.Set;

import com.google.common.collect.Sets;
import org.junit.Test;

import org.apache.cassandra.exceptions.SyntaxException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class ReservedTypeNamesTest
{
    @Test
    public void testUnquotedReservedTypeNames()
    {
        for (String reservedTypeName : ReservedTypeNames.reservedTypeNames)
        {
            assertTypeCreationParseFails(reservedTypeName);
        }
    }

    @Test
    public void testQuotedReservedTypeNames()
    {
        for (String reservedTypeName : ReservedTypeNames.reservedTypeNames)
        {
            String doubleQuotedName = CqlBuilder.maybeQuoteTypeName(reservedTypeName);
            assertEquals('"' + reservedTypeName + '"', doubleQuotedName);

            parseCreateType(doubleQuotedName);
        }
    }

    @Test
    public void testUnreservedNames()
    {
        Set<String> unreservedNames = Sets.newHashSet("mytype", "niceType", "hello", "cassandra");

        for (String unreservedName: unreservedNames)
        {
            assertFalse(ReservedTypeNames.isReserved(unreservedName));
            parseCreateType(unreservedName);
        }
    }

    private void assertTypeCreationParseFails(String typeName)
    {
        try
        {
            parseCreateType(typeName);
            fail(String.format("Reserved type name %s should not have parsed", typeName));
        }
        catch (SyntaxException exception)
        {
            // Expected
        }
    }

    private void parseCreateType(String typeName)
    {
        QueryProcessor.parseStatement(String.format("CREATE TYPE ks.%s (id int)", typeName));
    }
}