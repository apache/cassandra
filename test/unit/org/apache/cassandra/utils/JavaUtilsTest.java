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

import org.junit.Test;

import static org.junit.Assert.*;

public class JavaUtilsTest
{
    @Test
    public void testSupportExitOnOutOfMemory()
    {
        assertFalse(JavaUtils.supportExitOnOutOfMemory("1.7.0-ea")); // Early Access
        assertFalse(JavaUtils.supportExitOnOutOfMemory("1.7.0"));    // Major (GA)
        assertFalse(JavaUtils.supportExitOnOutOfMemory("1.7.0_20")); // Minor #1 (GA)
        assertFalse(JavaUtils.supportExitOnOutOfMemory("1.7.0_5"));  // Security #1 (GA)

        assertFalse(JavaUtils.supportExitOnOutOfMemory("1.7.0_95"));
        assertTrue(JavaUtils.supportExitOnOutOfMemory("1.7.0_101"));

        assertFalse(JavaUtils.supportExitOnOutOfMemory("1.8.0-ea")); // Early Access
        assertFalse(JavaUtils.supportExitOnOutOfMemory("1.8.0"));    // Major (GA)
        assertFalse(JavaUtils.supportExitOnOutOfMemory("1.8.0_20")); // Minor #1 (GA)
        assertFalse(JavaUtils.supportExitOnOutOfMemory("1.8.0_5"));  // Security #1 (GA)

        assertFalse(JavaUtils.supportExitOnOutOfMemory("1.8.0_91"));
        assertTrue(JavaUtils.supportExitOnOutOfMemory("1.8.0_92"));
        assertTrue(JavaUtils.supportExitOnOutOfMemory("1.8.0_101"));

        // Test based on http://openjdk.java.net/jeps/223
        assertTrue(JavaUtils.supportExitOnOutOfMemory("9-ea"));  // Early Access
        assertTrue(JavaUtils.supportExitOnOutOfMemory("9"));     // Major (GA)
        assertTrue(JavaUtils.supportExitOnOutOfMemory("9.1.2")); // Minor #1 (GA)
        assertTrue(JavaUtils.supportExitOnOutOfMemory("9.0.1")); // Security #1 (GA)
    }
}
