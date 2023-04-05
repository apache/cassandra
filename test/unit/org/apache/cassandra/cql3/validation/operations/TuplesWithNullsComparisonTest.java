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

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;

public class TuplesWithNullsComparisonTest extends CQLTester
{
    @Test
    public void testAddUDTField() throws Throwable
    {
        String typename = createType("create type %s (foo text);");
        createTable("create table %s (pk int, ck frozen<" + typename + ">, v int, primary key(pk, ck));");
        execute("insert into %s (pk, ck, v) values (0, system.fromjson('{\"foo\": \"foo\"}'), 0);");
        execute("ALTER TYPE " + KEYSPACE + '.' + typename + " ADD bar text;");
        execute("insert into %s (pk, ck, v) values (0, system.fromjson('{\"foo\": \"foo\"}'), 1);");
        execute("insert into %s (pk, ck, v) values (0, system.fromjson('{\"foo\": \"foo\", \"bar\": null}'), 2);");
        flush();
        compact();
        assertRows(execute("select v from %s where pk = 0 and ck=system.fromjson('{\"foo\": \"foo\"}')"),
                   row(2));
        assertRows(execute("select v from %s where pk = 0"),
                   row(2));
    }

    @Test
    public void testFieldWithData() throws Throwable
    {
        String typename = createType("create type %s (foo text);");
        createTable("create table %s (pk int, ck frozen<" + typename + ">, v int, primary key(pk, ck));");
        execute("insert into %s (pk, ck, v) values (0, system.fromjson('{\"foo\": \"foo\"}'), 1);");
        execute("ALTER TYPE " + KEYSPACE + '.' + typename + " ADD bar text;");
        // this row becomes inaccessible by primary key but remains visible through select *
        execute("insert into %s (pk, ck, v) values (0, system.fromjson('{\"foo\": \"foo\", \"bar\": \"bar\"}'), 2);");
        flush();
        compact();
        assertRows(execute("select v from %s where pk = 0"),
                   row(1),
                   row(2));
    }

    @Test
    public void testAddUDTFields() throws Throwable
    {
        String typename = createType("create type %s (foo text);");
        createTable("create table %s (pk int, ck frozen<" + typename + ">, v int, primary key(pk, ck));");
        execute("insert into %s (pk, ck, v) values (0, system.fromjson('{\"foo\": \"foo\"}'), 0);");
        execute("ALTER TYPE " + KEYSPACE + '.' + typename + " ADD bar text;");
        execute("ALTER TYPE " + KEYSPACE + '.' + typename + " ADD bar2 text;");
        execute("ALTER TYPE " + KEYSPACE + '.' + typename + " ADD bar3 text;");
        execute("insert into %s (pk, ck, v) values (0, system.fromjson('{\"foo\": \"foo\"}'), 1);");
        execute("insert into %s (pk, ck, v) values (0, system.fromjson('{\"foo\": \"foo\", \"bar\": null, \"bar2\": null, \"bar3\": null}'), 2);");
        flush();
        compact();
        assertRows(execute("select v from %s where pk = 0 and ck=system.fromjson('{\"foo\": \"foo\"}')"),
                   row(2));
        assertRows(execute("select v from %s where pk = 0"),
                   row(2));
    }
}
