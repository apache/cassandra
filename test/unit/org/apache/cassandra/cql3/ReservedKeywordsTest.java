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

import java.lang.reflect.Modifier;

import org.assertj.core.api.SoftAssertions;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.exceptions.SyntaxException;

public class ReservedKeywordsTest
{
    @Test
    public void testReservedWordsForColumns()
    {
        for (String reservedWord : ReservedKeywords.reservedKeywords)
        {
            if (isAllowed(reservedWord))
                Assert.fail(String.format("Reserved keyword %s should not have parsed", reservedWord));
        }
    }

    @Test
    public void parserAndTextFileMatch()
    {
        // If this test starts to fail that means that the lexer added a new keyword, and this keyword was not updated
        // to be unreserved.
        //
        // To mark a keyword as unreserved, open "Parser.g" and search for
        //    basic_unreserved_keyword returns [String str]
        // or
        //    unreserved_keyword returns [String str]
        // Add your keyword there and rebuild the jar (to generate the parser).
        //
        // If it is desired to make this keyword reserved, then you must first go to the mailing list and request a vote
        // on this change, if that vote passes then you can update "reserved_keywords.txt" (and pylib/cqlshlib/cqlhandling.py::cql_keywords_reserved).
        // Never update "reserved_keywords.txt" without a vote on the mailing list!
        SoftAssertions asserts = new SoftAssertions();
        for (var f : Cql_Lexer.class.getDeclaredFields())
        {
            if (!Modifier.isStatic(f.getModifiers())) continue;
            if (!f.getName().startsWith("K_")) continue;
            String name = f.getName();
            String keyword = name.replaceFirst("K_", "");

            asserts.assertThat(ReservedKeywords.isReserved(keyword))
                   .describedAs(keyword)
                   .isEqualTo(!isAllowed(keyword));
        }
        asserts.assertAll();
    }

    /**
     * Legacy USER and IDENTITY statements must accept unreserved keywords as names, just like
     * role statements do ({@code roleName} accepts {@code unreserved_keyword}). Otherwise adding
     * a new keyword to the grammar breaks existing deployments with a user or identity of that name.
     */
    @Test
    public void testUnreservedKeywordsAsUserNameAndIdentity()
    {
        SoftAssertions asserts = new SoftAssertions();
        for (var f : Cql_Lexer.class.getDeclaredFields())
        {
            if (!Modifier.isStatic(f.getModifiers())) continue;
            if (!f.getName().startsWith("K_")) continue;
            String keyword = f.getName().replaceFirst("K_", "");
            if (ReservedKeywords.isReserved(keyword)) continue;

            for (String statement : new String[]{ "DROP USER %s", "DROP IDENTITY %s" })
            {
                asserts.assertThat(parses(String.format(statement, keyword)))
                       .describedAs(String.format(statement, keyword))
                       .isTrue();
            }
        }
        asserts.assertAll();
    }

    private static boolean isAllowed(String keyword)
    {
        return parses(String.format("ALTER TABLE ks.t ADD %s TEXT", keyword));
    }

    private static boolean parses(String cql)
    {
        try
        {
            QueryProcessor.parseStatement(cql);
            return true;
        }
        catch (SyntaxException ignore)
        {
            return false;
        }
    }
}
