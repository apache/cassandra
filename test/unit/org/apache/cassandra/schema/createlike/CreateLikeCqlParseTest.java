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

package org.apache.cassandra.schema.createlike;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.exceptions.SyntaxException;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class CreateLikeCqlParseTest extends CQLTester
{
    // Incorrect use of create table like cql
    private static final String[] unSupportedCqls = new String[]
            {
                    "CREATE TABLE ta (a int primary key, b int) LIKE tb", // useless column information
                    "CREATE TABLE ta (a int primary key, b int MASKED WITH DEFAULT) LIKE tb",
                    "CREATE TABLE IF NOT EXISTS LIKE tb", // missing target table
                    "CREATE TABLE IF NOT EXISTS LIKE tb WITH compression = { 'enabled' : 'false'}",
                    "CREATE TABLE ta IF NOT EXISTS LIKE ", // missing source table
                    "CREATE TABLE ta LIKE WITH id = '123-111'" // id is not supported
            };

    @Test
    public void testUnsupportedCqlParse()
    {
        for (String cql : unSupportedCqls)
        {
            assertThatExceptionOfType(SyntaxException.class)
                    .isThrownBy(() -> QueryProcessor.parseStatement(cql));
        }
    }
}
