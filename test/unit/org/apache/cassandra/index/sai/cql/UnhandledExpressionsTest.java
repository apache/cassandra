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

package org.apache.cassandra.index.sai.cql;

import org.junit.Test;

import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.SAITester;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class UnhandledExpressionsTest extends SAITester
{
    @Test
    public void unhandledOperatorsOnIndexedColumnAreHandledCorrectly()
    {
        createTable("CREATE TABLE %s (pk int primary key, val1 int, val2 text)");
        createIndex("CREATE INDEX ON %s(val1) USING 'sai'");
        createIndex("CREATE INDEX ON %s(val2) USING 'sai'");

        execute("INSERT INTO %s (pk, val1, val2) VALUES (1, 1, '11')");
        execute("INSERT INTO %s (pk, val1, val2) VALUES (2, 2, '22')");
        execute("INSERT INTO %s (pk, val1, val2) VALUES (3, 3, '33')");
        execute("INSERT INTO %s (pk, val1, val2) VALUES (4, 4, '44')");

        // The LT, LTE, GTE, GT & IN operators are allowed without an index but need ALLOW FILTERING
        assertRows(execute("SELECT pk FROM %s WHERE val1 = 1 AND val2 < '22' ALLOW FILTERING"), row(1));
        assertRows(execute("SELECT pk FROM %s WHERE val1 = 1 AND val2 <= '11' ALLOW FILTERING"), row(1));
        assertRows(execute("SELECT pk FROM %s WHERE val1 = 1 AND val2 >= '11' ALLOW FILTERING"), row(1));
        assertRows(execute("SELECT pk FROM %s WHERE val1 = 1 AND val2 > '00' ALLOW FILTERING"), row(1));
        assertRows(execute("SELECT pk FROM %s WHERE val1 = 1 AND val2 in ('11', '22') ALLOW FILTERING"), row(1));

        // The LIKE operator is rejected because it needs to be handled by an index
        assertThatThrownBy(() -> execute("SELECT pk FROM %s WHERE val1 = 1 AND val2 like '1%%'"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("LIKE restriction is only supported on properly indexed columns");

        // The IS NOT operator is only valid on materialized views
        assertThatThrownBy(() -> execute("SELECT pk FROM %s WHERE val1 = 1 AND val2 is not null"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("Unsupported restriction:");

        // The != operator is currently not supported at all
        assertThatThrownBy(() -> execute("SELECT pk FROM %s WHERE val1 = 1 AND val2 != '22'"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("Unsupported \"!=\" relation:");
    }
}
