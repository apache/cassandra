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
package org.apache.cassandra.cql3;

import static junit.framework.Assert.fail;

import org.junit.Test;

import org.apache.cassandra.config.Config.DiskFailurePolicy;
import org.apache.cassandra.config.DatabaseDescriptor;

/**
 * Test that exceptions during flush are treated according to the disk failure policy.
 */
public class OutOfSpaceIgnoreTest extends OutOfSpaceBase
{
    @Test
    public void testFlushUnwriteableIgnore() throws Throwable
    {
        makeTable();
        markDirectoriesUnwriteable();

        DiskFailurePolicy oldPolicy = DatabaseDescriptor.getDiskFailurePolicy();
        try
        {
            DatabaseDescriptor.setDiskFailurePolicy(DiskFailurePolicy.ignore);
            flushAndExpectError();
        }
        finally
        {
            DatabaseDescriptor.setDiskFailurePolicy(oldPolicy);
        }

        makeTable();
        try
        {
            flush();
            fail("Subsequent flushes expected to fail.");
        }
        catch (RuntimeException e)
        {
            // correct path
        }
    }
}
