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
package org.apache.cassandra.db;

import org.junit.Test;

import org.assertj.core.api.Assertions;

public class PartitionRangeReadCommandCQLTest extends ReadCommandCQLTester<PartitionRangeReadCommand>
{
    @Test
    public void testToCQLString()
    {
        createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c))");

        assertToCQLString("SELECT * FROM %s", "SELECT * FROM %s ALLOW FILTERING");

        assertToCQLString("SELECT * FROM %s WHERE c = 0 ALLOW FILTERING", "SELECT * FROM %s WHERE c = 0 ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE (c) = (0) ALLOW FILTERING", "SELECT * FROM %s WHERE c = 0 ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE c > 0 ALLOW FILTERING", "SELECT * FROM %s WHERE c > 0 ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE c < 0 ALLOW FILTERING", "SELECT * FROM %s WHERE c < 0 ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE c >= 0 ALLOW FILTERING", "SELECT * FROM %s WHERE c >= 0 ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE c <= 0 ALLOW FILTERING", "SELECT * FROM %s WHERE c <= 0 ALLOW FILTERING");

        assertToCQLString("SELECT * FROM %s WHERE v = 1 ALLOW FILTERING", "SELECT * FROM %s WHERE v = 1 ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE c = 0 AND v = 1 ALLOW FILTERING", "SELECT * FROM %s WHERE c = 0 AND v = 1 ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE c > 0 AND v = 1 ALLOW FILTERING", "SELECT * FROM %s WHERE c > 0 AND v = 1 ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE c < 0 AND v = 1 ALLOW FILTERING", "SELECT * FROM %s WHERE c < 0 AND v = 1 ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE c >= 0 AND v = 1 ALLOW FILTERING", "SELECT * FROM %s WHERE c >= 0 AND v = 1 ALLOW FILTERING");
        assertToCQLString("SELECT * FROM %s WHERE c <= 0 AND v = 1 ALLOW FILTERING", "SELECT * FROM %s WHERE c <= 0 AND v = 1 ALLOW FILTERING");
    }

    @Override
    protected PartitionRangeReadCommand parseCommand(String query)
    {
        ReadCommand command = parseReadCommand(query);
        Assertions.assertThat(command).isInstanceOf(PartitionRangeReadCommand.class);
        return (PartitionRangeReadCommand) command;
    }
}
