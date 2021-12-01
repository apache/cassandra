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

package org.apache.cassandra.utils;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;


public class ImmutableUtilsTest
{
    @Test
    public void without()
    {
        ImmutableMap<String, Integer> m0 = ImmutableMap.of("a", 1, "b", 2);
        ImmutableMap<String, Integer> m1 = ImmutableUtils.without(m0, "b");
        ImmutableMap<String, Integer> m2 = ImmutableUtils.without(m0, "c");

        assertThat(m1).containsEntry("a", 1).doesNotContainKey("b");
        assertThat(m2).containsEntry("a", 1).containsEntry("b", 2).doesNotContainKey("c");
        assertThat(m2).isEqualTo(m0);
    }

    @Test
    public void withAdded()
    {
        ImmutableMap<String, Integer> m0 = ImmutableMap.of("a", 1, "b", 2);
        ImmutableMap<String, Integer> m1 = ImmutableUtils.withAddedOrUpdated(m0, "b", 3);
        ImmutableMap<String, Integer> m2 = ImmutableUtils.withAddedOrUpdated(m0, "c", 3);

        assertThat(m1).containsEntry("a", 1).containsEntry("b", 3);
        assertThat(m2).containsEntry("a", 1).containsEntry("b", 2).containsEntry("c", 3);
    }
}
