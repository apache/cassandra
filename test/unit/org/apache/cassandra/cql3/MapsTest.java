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

package org.apache.cassandra.cql3;

import java.util.function.Function;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.NumberType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.Pair;

public class MapsTest extends CQLTester
{
    private final Function<NumberType<?>, AbstractType<?>> identityMapper = integerType -> integerType;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testGetExactMapTypeIfKnownWithDifferentTypes()
    {
        thrown.expect(InvalidRequestException.class);
        thrown.expectMessage("Invalid collection literal: all selectors must have the same CQL type inside collection literals");

        Maps.getExactMapTypeIfKnown(ImmutableList.of(
            Pair.create(Int32Type.instance, Int32Type.instance),
            Pair.create(Int32Type.instance, IntegerType.instance)
        ), identityMapper);
    }

    @Test
    public void testGetExactMapTypeIfKnownWithTheSameTypes()
    {
        AbstractType<?> exactType = Maps.getExactMapTypeIfKnown(ImmutableList.of(
            Pair.create(Int32Type.instance, Int32Type.instance),
            Pair.create(Int32Type.instance, Int32Type.instance)
        ), identityMapper);

        AbstractType<?> expected = MapType.getInstance(Int32Type.instance, Int32Type.instance, false).freeze();
        Assert.assertEquals(expected, exactType);
    }

    @Test
    public void testGetExactMapTypeIfKnownWithoutTypes()
    {
        AbstractType<?> exactType = Maps.getExactMapTypeIfKnown(ImmutableList.of(), identityMapper);

        Assert.assertNull(exactType);
    }
}