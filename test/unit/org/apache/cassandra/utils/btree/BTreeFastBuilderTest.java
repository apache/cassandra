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

package org.apache.cassandra.utils.btree;

import java.util.List;

import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.assertj.core.api.Assertions;

@RunWith(Parameterized.class)
public class BTreeFastBuilderTest
{
    @Parameterized.Parameter(0)
    public int addedElements;

    @Parameterized.Parameters(name = "addedElements={0}")
    public static List<Object> parameters()
    {
        return ImmutableList.of(1,
                                BTree.MIN_KEYS - 1, BTree.MIN_KEYS, BTree.MIN_KEYS + 1,
                                BTree.MAX_KEYS - 1, BTree.MAX_KEYS, BTree.MAX_KEYS + 1,
                                BTree.MAX_KEYS * 100);
    }

    // we want all references to values cleared within the pooled builder hierarchy to avoid memory leaking
    @Test
    public void testPooledBuffersCleanup()
    {
        try (BTree.FastBuilder<Object> builder = BTree.fastBuilder())
        {
            for (int i = 0; i < addedElements; i++)
            {
                builder.add(i);
            }
            builder.build();
        }

        try (BTree.FastBuilder<Object> builder = BTree.fastBuilder())
        {
            BTree.LeafOrBranchBuilder builderToCheck = builder;
            int level = 0;
            while (builderToCheck != null)
            {
                Assertions.assertThat(builderToCheck.buffer)
                          .as("Failed for buffer on level = " + level)
                          .containsOnlyNulls();
                if (builderToCheck.savedBuffer != null)
                {
                    Assertions.assertThat(builderToCheck.savedBuffer)
                              .as("Failed for savedBuffer on level = " + level)
                              .containsOnlyNulls();
                }
                Assertions.assertThat(builderToCheck.savedNextKey).isNull();
                builderToCheck = builderToCheck.parent;
                level++;
            }
        }
    }
}
