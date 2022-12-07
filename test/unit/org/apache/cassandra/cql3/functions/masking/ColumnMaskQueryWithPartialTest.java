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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.junit.runners.Parameterized;

/**
 * {@link ColumnMaskQueryTester} for {@link PartialMaskingFunction}.
 */
public class ColumnMaskQueryWithPartialTest extends ColumnMaskQueryTester
{
    @Parameterized.Parameters(name = "order={0}, mask={1}, type={2}, value={3}")
    public static Collection<Object[]> options()
    {
        List<Object[]> options = new ArrayList<>();
        for (String order : Arrays.asList("ASC", "DESC"))
        {
            options.add(new Object[]{ order, "mask_inner(null, null)", "text", "abcdef", "******" });
            options.add(new Object[]{ order, "mask_inner(1, 2)", "text", "abcdef", "a***ef" });
            options.add(new Object[]{ order, "mask_inner(1, 2, '#')", "text", "abcdef", "a###ef" });
            options.add(new Object[]{ order, "mask_outer(1, null)", "text", "abcdef", "*bcdef" });
            options.add(new Object[]{ order, "mask_outer(1, 2)", "text", "abcdef", "*bcd**" });
            options.add(new Object[]{ order, "mask_outer(1, 2, '#')", "text", "abcdef", "#bcd##", });
        }
        return options;
    }
}
