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
package org.apache.cassandra.cql3.validation.miscellaneous;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.exceptions.SyntaxException;

public class PgStringTest extends CQLTester
{
    @Test
    public void testPgSyleFunction() throws Throwable
    {
        execute("create or replace function "+KEYSPACE+".pgfun1 ( input double ) called on null input returns text language java\n" +
                "AS $$return \"foobar\";$$");
    }

    @Test
    public void testPgSyleInsert() throws Throwable
    {
        createTable("CREATE TABLE %s (key ascii primary key, val text)");

        // some non-terminated pg-strings
        assertInvalidSyntax("INSERT INTO %s (key, val) VALUES ($ $key_empty$$, $$'' value for empty$$)");
        assertInvalidSyntax("INSERT INTO %s (key, val) VALUES ($$key_empty$$, $$'' value for empty$ $)");
        assertInvalidSyntax("INSERT INTO %s (key, val) VALUES ($$key_empty$ $, $$'' value for empty$$)");

        // different pg-style markers for multiple strings
        execute("INSERT INTO %s (key, val) VALUES ($$prim$ $ $key$$, $$some '' arbitrary value$$)");
        // same empty pg-style marker for multiple strings
        execute("INSERT INTO %s (key, val) VALUES ($$key_empty$$, $$'' value for empty$$)");
        // stange but valid pg-style
        execute("INSERT INTO %s (key, val) VALUES ($$$foo$_$foo$$, $$$'' value for empty$$)");
        // these are conventional quoted strings
        execute("INSERT INTO %s (key, val) VALUES ('$txt$key$$$$txt$', '$txt$'' other value$txt$')");

        assertRows(execute("SELECT key, val FROM %s WHERE key='prim$ $ $key'"),
                   row("prim$ $ $key", "some '' arbitrary value")
        );
        assertRows(execute("SELECT key, val FROM %s WHERE key='key_empty'"),
                   row("key_empty", "'' value for empty")
        );
        assertRows(execute("SELECT key, val FROM %s WHERE key='$foo$_$foo'"),
                   row("$foo$_$foo", "$'' value for empty")
        );
        assertRows(execute("SELECT key, val FROM %s WHERE key='$txt$key$$$$txt$'"),
                   row("$txt$key$$$$txt$", "$txt$' other value$txt$")
        );

        // invalid syntax
        assertInvalidSyntax("INSERT INTO %s (key, val) VALUES ($ascii$prim$$$key$invterm$, $txt$some '' arbitrary value$txt$)");
    }

    @Test(expected = SyntaxException.class)
    public void testMarkerPgFail() throws Throwable
    {
        // must throw SyntaxException - not StringIndexOutOfBoundsException or similar
        execute("create function "+KEYSPACE+".pgfun1 ( input double ) called on null input returns bigint language java\n" +
                "AS $javasrc$return 0L;$javasrc$;");
    }
}
