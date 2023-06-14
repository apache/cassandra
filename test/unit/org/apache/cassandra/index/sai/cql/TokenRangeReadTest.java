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

import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.StorageAttachedIndex;

import static java.lang.String.format;

public class TokenRangeReadTest extends SAITester
{
    @Test
    public void testTokenRangeRead() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, v1 text, PRIMARY KEY (k1))");
        createIndex(format("CREATE CUSTOM INDEX ON %%s(v1) USING '%s'", StorageAttachedIndex.class.getName()));

        execute("INSERT INTO %S(k1, v1) values(1, '1')");

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT * FROM %s WHERE v1 = '1'"), row(1, "1"));
            assertRows(execute("SELECT * FROM %s WHERE token(k1) >= token(1) AND token(k1) <= token(1)"), row(1, "1"));
            assertRows(execute("SELECT * FROM %s WHERE token(k1) >= token(1) AND token(k1) <= token(1) AND v1 = '1'"), row(1, "1"));
        });
    }
}
