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

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.VectorType;

/**
 * {@link ColumnMaskQueryTester} for {@link DefaultMaskingFunction}.
 */
public class ColumnMaskQueryWithDefaultTest extends ColumnMaskQueryTester
{
    @Parameterized.Parameters(name = "order={0}, mask={1}, type={2}, value={3}")
    public static Collection<Object[]> options()
    {
        List<Object[]> options = new ArrayList<>();
        for (String order : Arrays.asList("ASC", "DESC"))
        {
            options.add(new Object[]{ order, "DEFAULT", "text", "abc", "****" });
            options.add(new Object[]{ order, "DEFAULT", "int", 123, 0 });
            options.add(new Object[]{ order, "mask_default()", "text", "abc", "****" });
            options.add(new Object[]{ order, "mask_default()", "int", 123, 0 });
            // TODO: the driver version that we use doesn't support vectors, so we have to use raw values by now
            options.add(new Object[]{ order, "mask_default()", "vector<int, 2>",
                                      VectorType.getInstance(Int32Type.instance, 2).decompose(1, 2),
                                      VectorType.getInstance(Int32Type.instance, 2).decompose(0, 0) });
        }
        return options;
    }
}
