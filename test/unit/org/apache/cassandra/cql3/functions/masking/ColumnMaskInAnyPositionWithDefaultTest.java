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

import static java.util.Collections.emptyList;

/**
 * {@link ColumnMaskInAnyPositionTester} for {@link DefaultMaskingFunction}.
 */
public class ColumnMaskInAnyPositionWithDefaultTest extends ColumnMaskInAnyPositionTester
{
    @Parameterized.Parameters(name = "mask={0}, type={1}")
    public static Collection<Object[]> options()
    {
        return Arrays.asList(new Object[][]{
        { "DEFAULT", "int", emptyList(), emptyList() },
        { "DEFAULT", "text", emptyList(), emptyList() },
        { "DEFAULT", "frozen<list<uuid>>", emptyList(), emptyList() },
        { "mask_default()", "int", emptyList(), emptyList() },
        { "mask_default()", "text", emptyList(), emptyList() },
        { "mask_default()", "frozen<list<uuid>>", emptyList(), emptyList() },
        });
    }
}
