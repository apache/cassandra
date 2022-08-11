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

import java.nio.file.Paths;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class HeapUtilsTest
{
    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void shouldDumpHeapWithPathArgSpecified()
    {
        DatabaseDescriptor.setDumpHeapOnUncaughtException(true);
        String path = HeapUtils.maybeCreateHeapDump();
        assertNotNull(path);
        assertTrue(Paths.get(path).toFile().exists());
        assertFalse(DatabaseDescriptor.getDumpHeapOnUncaughtException());

        // After the first dump, subsequent requests should be no-ops...
        path = HeapUtils.maybeCreateHeapDump();
        assertNull(path);

        // ...until creation is manually re-enabled.
        DatabaseDescriptor.setDumpHeapOnUncaughtException(true);
        assertTrue(DatabaseDescriptor.getDumpHeapOnUncaughtException());
        assertNotNull(DatabaseDescriptor.getHeapDumpPath());
        path = HeapUtils.maybeCreateHeapDump();
        assertNotNull(path);
        assertTrue(Paths.get(path).toFile().exists());
        assertFalse(DatabaseDescriptor.getDumpHeapOnUncaughtException());
    }
}