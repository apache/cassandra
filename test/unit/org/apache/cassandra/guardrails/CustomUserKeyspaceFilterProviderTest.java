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

package org.apache.cassandra.guardrails;


import org.junit.Test;

import org.apache.cassandra.service.QueryState;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class CustomUserKeyspaceFilterProviderTest
{
    public static class TestUserKeyspaceFilterProvider implements UserKeyspaceFilterProvider
    {
        @Override
        public UserKeyspaceFilter get(QueryState queryState)
        {
            return null;
        }
    }

    @Test
    public void testMakeWithValidClass()
    {
        String validClassName = "org.apache.cassandra.guardrails.CustomUserKeyspaceFilterProviderTest$TestUserKeyspaceFilterProvider";
        UserKeyspaceFilterProvider provider = CustomUserKeyspaceFilterProvider.make(validClassName);
        assertTrue(provider instanceof TestUserKeyspaceFilterProvider);
    }

    @Test
    public void testMakeWithInvalidClass()
    {
        String invalidClassName = "com.example.NonExistingClass";
        assertThrows(IllegalStateException.class, () -> CustomUserKeyspaceFilterProvider.make(invalidClassName));
    }
}
