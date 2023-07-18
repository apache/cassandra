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

package org.apache.cassandra.cql3.validation.entities;

import java.math.BigInteger;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.exceptions.InvalidRequestException;

public class CQLVectorTest extends CQLTester
{
    @Test
    public void invalidNumberOfDimensionsFixedWidth() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int primary key, value vector<int, 2>)");

        // fewer values than expected, with literals and bind markers
        assertInvalidThrowMessage("Invalid vector literal for value of type vector<int, 2>; expected 2 elements, but given 1",
                                  InvalidRequestException.class,
                                  "INSERT INTO %s (pk, value) VALUES (0, [1])");
        assertInvalidThrowMessage("Not enough bytes to read a vector<int, 2>",
                                  InvalidRequestException.class,
                                  "INSERT INTO %s (pk, value) VALUES (0, ?)", vector(1));

        // more values than expected, with literals and bind markers
        assertInvalidThrowMessage("Invalid vector literal for value of type vector<int, 2>; expected 2 elements, but given 3",
                                  InvalidRequestException.class,
                                  "INSERT INTO %s (pk, value) VALUES (0, [1, 2, 3])");
        assertInvalidThrowMessage("Unexpected 4 extraneous bytes after vector<int, 2> value",
                                  InvalidRequestException.class,
                                  "INSERT INTO %s (pk, value) VALUES (0, ?)", vector(1, 2, 3));
    }

    @Test
    public void invalidNumberOfDimensionsVariableWidth() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int primary key, value vector<text, 2>)");

        // fewer values than expected, with literals and bind markers
        assertInvalidThrowMessage("Invalid vector literal for value of type vector<text, 2>; expected 2 elements, but given 1",
                                  InvalidRequestException.class,
                                  "INSERT INTO %s (pk, value) VALUES (0, ['a'])");
        assertInvalidThrowMessage("Not enough bytes to read a vector<text, 2>",
                                  InvalidRequestException.class,
                                  "INSERT INTO %s (pk, value) VALUES (0, ?)", vector("a"));

        // more values than expected, with literals and bind markers
        assertInvalidThrowMessage("Invalid vector literal for value of type vector<text, 2>; expected 2 elements, but given 3",
                                  InvalidRequestException.class,
                                  "INSERT INTO %s (pk, value) VALUES (0, ['a', 'b', 'c'])");
        assertInvalidThrowMessage("Unexpected 2 extraneous bytes after vector<text, 2> value",
                                  InvalidRequestException.class,
                                  "INSERT INTO %s (pk, value) VALUES (0, ?)", vector("a", "b", "c"));
    }

    @Test
    public void invalidElementTypeFixedWidth() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int primary key, value vector<int, 2>)");

        // fixed-length bigint instead of int, with literals and bind markers
        assertInvalidThrowMessage("Invalid vector literal for value: value (bigint)1 is not of type int",
                                  InvalidRequestException.class,
                                  "INSERT INTO %s (pk, value) VALUES (0, [(bigint) 1, (bigint) 2])");
        assertInvalidThrowMessage("Unexpected 8 extraneous bytes after vector<int, 2> value",
                                  InvalidRequestException.class,
                                  "INSERT INTO %s (pk, value) VALUES (0, ?)", vector(1L, Long.MAX_VALUE));

        // variable-length text instead of int, with literals and bind markers
        assertInvalidThrowMessage("Invalid vector literal for value: value 'a' is not of type int",
                                  InvalidRequestException.class,
                                  "INSERT INTO %s (pk, value) VALUES (0, ['a', 'b'])");
        assertInvalidThrowMessage("Not enough bytes to read a vector<int, 2>",
                                  InvalidRequestException.class,
                                  "INSERT INTO %s (pk, value) VALUES (0, ?)", vector("a", "b"));
    }

    @Test
    public void invalidElementTypeVariableWidth() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int primary key, value vector<text, 2>)");

        // fixed-length int instead of text, with literals and bind markers
        assertInvalidThrowMessage("Invalid vector literal for value: value 1 is not of type text",
                                  InvalidRequestException.class,
                                  "INSERT INTO %s (pk, value) VALUES (0, [1, 2])");
        assertInvalidThrowMessage("Unexpected 6 extraneous bytes after vector<text, 2> value",
                                  InvalidRequestException.class,
                                  "INSERT INTO %s (pk, value) VALUES (0, ?)", vector(1, 2));

        // variable-length varint instead of text, with literals and bind markers
        assertInvalidThrowMessage("Invalid vector literal for value: value (varint)1 is not of type text",
                                  InvalidRequestException.class,
                                  "INSERT INTO %s (pk, value) VALUES (0, [(varint) 1, (varint) 2])");
        assertInvalidThrowMessage("String didn't validate.",
                                  InvalidRequestException.class,
                                  "INSERT INTO %s (pk, value) VALUES (0, ?)",
                                  vector(BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE), BigInteger.ONE));
    }
}
