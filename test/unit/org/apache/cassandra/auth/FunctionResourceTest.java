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
package org.apache.cassandra.auth;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.TypeParser;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.Assert.assertEquals;

public class FunctionResourceTest
{
    private static final String ks = "fr_ks";
    private static final String func = "functions";
    private static final String name = "concat";
    private static final String varType = "org.apache.cassandra.db.marshal.UTF8Type";

    @Test
    public void testFunction() throws Exception
    {
        FunctionResource expected = FunctionResource.root();
        FunctionResource actual = FunctionResource.fromName(func);
        assertEquals(expected, actual);
        assertEquals(expected.getName(), actual.getName());
    }

    @Test
    public void testFunctionKeyspace() throws Exception
    {
        FunctionResource expected = FunctionResource.keyspace(ks);
        FunctionResource actual = FunctionResource.fromName(String.format("%s/%s", func, ks));
        assertEquals(expected, actual);
        assertEquals(expected.getKeyspace(), actual.getKeyspace());
    }

    @Test
    public void testFunctionWithSingleInputParameter() throws Exception
    {
        List<AbstractType<?>> argTypes = new ArrayList<>();
        argTypes.add(TypeParser.parse(varType));
        FunctionResource expected = FunctionResource.function(ks, name, argTypes);
        FunctionResource actual = FunctionResource.fromName(String.format("%s/%s/%s[%s]", func, ks, name, varType));
        assertEquals(expected, actual);
        assertEquals(expected.getKeyspace(), actual.getKeyspace());
    }

    @Test
    public void testFunctionWithMultipleInputParameter() throws Exception
    {
        List<AbstractType<?>> argTypes = new ArrayList<>();
        argTypes.add(TypeParser.parse(varType));
        argTypes.add(TypeParser.parse(varType));
        FunctionResource expected = FunctionResource.function(ks, name, argTypes);
        FunctionResource actual = FunctionResource.fromName(String.format("%s/%s/%s[%s^%s]", func, ks, name, varType, varType));
        assertEquals(expected, actual);
        assertEquals(expected.getKeyspace(), actual.getKeyspace());
    }

    @Test
    public void testFunctionWithoutInputParameter() throws Exception
    {
        List<AbstractType<?>> argTypes = new ArrayList<>();
        FunctionResource expected = FunctionResource.function(ks, name, argTypes);
        FunctionResource actual = FunctionResource.fromName(String.format("%s/%s/%s[]", func, ks, name));
        assertEquals(expected, actual);
        assertEquals(expected.getKeyspace(), actual.getKeyspace());
    }

    @Test
    public void testInvalidFunctionName()
    {
        String expected = "functions_test is not a valid function resource name";
        assertThatExceptionOfType(IllegalArgumentException.class)
            .describedAs(expected)
            .isThrownBy(() -> FunctionResource.fromName("functions_test"));
    }

    @Test
    public void testFunctionWithInvalidInput()
    {
        String expected = String.format("%s/%s/%s[%s]/test is not a valid function resource name", func, ks, name, varType);
        assertThatExceptionOfType(IllegalArgumentException.class)
            .describedAs(expected)
            .isThrownBy(() -> FunctionResource.fromName(String.format("%s/%s/%s[%s]/test", func, ks, name, varType)));
    }
}