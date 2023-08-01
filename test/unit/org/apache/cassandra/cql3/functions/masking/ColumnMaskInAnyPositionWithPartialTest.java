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

import java.util.Arrays;
import java.util.Collection;

import org.junit.runners.Parameterized;

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;

import static java.util.Arrays.asList;

/**
 * {@link ColumnMaskInAnyPositionTester} for {@link PartialMaskingFunction}.
 */
public class ColumnMaskInAnyPositionWithPartialTest extends ColumnMaskInAnyPositionTester
{
    @Parameterized.Parameters(name = "mask={0}, type={1}")
    public static Collection<Object[]> options()
    {
        return Arrays.asList(new Object[][]{
        { "mask_inner(1, 2)", "text",
          asList(Int32Type.instance, Int32Type.instance),
          asList(Int32Type.instance.decompose(1), Int32Type.instance.decompose(2)) },
        { "mask_outer(1, 2)", "text",
          asList(Int32Type.instance, Int32Type.instance),
          asList(Int32Type.instance.decompose(1), Int32Type.instance.decompose(2)) },
        { "mask_inner(1, 2, '#')", "text",
          asList(Int32Type.instance, Int32Type.instance, UTF8Type.instance),
          asList(Int32Type.instance.decompose(1), Int32Type.instance.decompose(2), UTF8Type.instance.decompose("#")) },
        { "mask_outer(1, 2, '#')", "text",
          asList(Int32Type.instance, Int32Type.instance, UTF8Type.instance),
          asList(Int32Type.instance.decompose(1), Int32Type.instance.decompose(2), UTF8Type.instance.decompose("#")) }
        });
    }
}
