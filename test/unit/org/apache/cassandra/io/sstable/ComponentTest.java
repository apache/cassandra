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

package org.apache.cassandra.io.sstable;

import org.junit.Test;

import static org.junit.Assert.*;

public class ComponentTest
{
    @Test
    public void testTypeCreationFromRepresentation_ValidId()
    {
        for (int i = 1; i < 10; i++)
            assertTrue(Component.Type.fromRepresentation((byte) i) != Component.Type.CUSTOM);
    }

    @Test
    public void testTypeCreationFromRepresentation_InvalidIds()
    {
        assertTrue(Component.Type.fromRepresentation((byte) -1) == Component.Type.CUSTOM);
        assertTrue(Component.Type.fromRepresentation((byte) 11) == Component.Type.CUSTOM);
        assertTrue(Component.Type.fromRepresentation((byte) 12) == Component.Type.CUSTOM);
    }
}
