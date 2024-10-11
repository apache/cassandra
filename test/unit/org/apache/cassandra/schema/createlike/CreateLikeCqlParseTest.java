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
    private static final String[] unSupportCqls = new String[]
            {
                    "create table if not exist ta like tb",
                    "create table ta (a int primary key, b int) like tb",
                    "create table ta (a int primary key, b int) like tb with comment = 'asss'",
                    "create table ta (a int primary key, b int) like tb with compaction = {'class':'UnifiedCompactionStrategy'}"
            };

    @Test
    public void testUnsupportCqlParse()
    {
        for (String cql : unSupportCqls)
        {
            assertThatExceptionOfType(SyntaxException.class)
                    .isThrownBy(() -> QueryProcessor.parseStatement(cql));
        }
    }
}
