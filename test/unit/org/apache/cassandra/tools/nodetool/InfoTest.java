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
package org.apache.cassandra.tools.nodetool;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.datastax.driver.core.PreparedStatement;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.tools.ToolRunner;

import static org.assertj.core.api.Assertions.assertThat;

public class InfoTest extends CQLTester
{
    private static final Pattern PREPARED_STATEMENT_CACHE_PATTERN =
    Pattern.compile("Prepared Stmt Cache\\s+: entries (\\d+), size ([^,]+), capacity ([^,]+), (\\d+) executions, (\\d+) evictions");

    @BeforeClass
    public static void setup() throws Exception
    {
        requireNetwork();
        startJMXServer();
    }

    @Test
    public void testInfoContainsPreparedStatementCache()
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, val text)");
        PreparedStatement preparedStatement = sessionNet().prepare("INSERT INTO " + KEYSPACE + '.' + currentTable() + " (id, val) VALUES (?, ?)");
        sessionNet().execute(preparedStatement.bind(1, "value1"));

        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("info");
        tool.assertOnCleanExit();
        String stdout = tool.getStdout();
        assertThat(stdout).contains("Prepared Stmt Cache");
        Matcher matcher = PREPARED_STATEMENT_CACHE_PATTERN.matcher(stdout);
        assertThat(matcher.find()).isTrue();
        assertThat(Integer.parseInt(matcher.group(1))).isGreaterThan(0);
        assertThat(matcher.group(2)).isNotEqualTo("0 bytes");
        assertThat(Integer.parseInt(matcher.group(4))).isGreaterThan(0);
    }
}
