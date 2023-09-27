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

package org.apache.cassandra.cql3.functions.masking;

import java.math.BigInteger;
import java.nio.ByteBuffer;

import org.junit.Test;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;

import static java.lang.String.format;

/**
 * Tests for {@link ReplaceMaskingFunction}.
 */
public class ReplaceMaskingFunctionTest extends MaskingFunctionTester
{
    @Override
    protected void testMaskingOnColumn(String name, CQL3Type type, Object value)
    {
        // null replacement argument
        assertRows(execute(format("SELECT mask_replace(%s, ?) FROM %%s", name), (Object) null),
                   row((Object) null));

        // not-null replacement argument
        AbstractType<?> t = type.getType();
        ByteBuffer replacementValue = t.getMaskedValue();
        String query = format("SELECT mask_replace(%s, ?) FROM %%s", name);
        assertRows(execute(query, replacementValue), row(replacementValue));
    }

    @Test
    public void testReplaceWithDifferentType() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v varint)");
        execute("INSERT INTO %s(k) VALUES (0)");
        assertRows(execute("SELECT mask_replace(v, 1) FROM %s"), row(BigInteger.ONE));
        assertRows(execute("SELECT mask_replace(v, (int) 1) FROM %s"), row(BigInteger.ONE));
        assertRows(execute("SELECT mask_replace(v, (bigint) 1) FROM %s"), row(BigInteger.ONE));
        assertRows(execute("SELECT mask_replace(v, (varint) 1) FROM %s"), row(BigInteger.ONE));
        assertRows(execute("SELECT mask_replace(v, ?) FROM %s", 1), row(BigInteger.ONE));
        assertRows(execute("SELECT mask_replace(v, ?) FROM %s", 1L), row(BigInteger.ONE));
        assertRows(execute("SELECT mask_replace(v, ?) FROM %s", BigInteger.ONE), row(BigInteger.ONE));
        assertInvalidThrowMessage("Type error: 1.2 cannot be passed as argument 1 of function system.mask_replace of type varint",
                                  InvalidRequestException.class, "SELECT mask_replace(v, 1.2) FROM %s");
        assertInvalidThrowMessage("Type error: 'secret' cannot be passed as argument 1 of function system.mask_replace of type varint",
                                  InvalidRequestException.class, "SELECT mask_replace(v, 'secret') FROM %s");

        createTable("CREATE TABLE %s (k int PRIMARY KEY, v varchar)");
        execute("INSERT INTO %s(k) VALUES (0)");
        assertRows(execute("SELECT mask_replace(v, 'secret') FROM %s"), row("secret"));
        assertRows(execute("SELECT mask_replace(v, (ascii) 'secret') FROM %s"), row("secret"));
        assertRows(execute("SELECT mask_replace(v, (text) 'secret') FROM %s"), row("secret"));
        assertRows(execute("SELECT mask_replace(v, (varchar) 'secret') FROM %s"), row("secret"));
        assertInvalidThrowMessage("Type error: 1 cannot be passed as argument 1 of function system.mask_replace of type text",
                                  InvalidRequestException.class, "SELECT mask_replace(v, 1) FROM %s");
    }
}
