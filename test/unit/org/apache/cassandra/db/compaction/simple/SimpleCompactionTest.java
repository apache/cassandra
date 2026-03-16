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

package org.apache.cassandra.db.compaction.simple;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ExecutionException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.Config.DiskAccessMode;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.utils.TestHelper;


@Ignore
@RunWith(Parameterized.class)
public abstract class SimpleCompactionTest extends CQLTester
{
    @Parameterized.Parameter(0)
    public DiskAccessMode compactionReadDiskAccessMode;

    @Parameterized.Parameter(1)
    public boolean cursorCompactionEnabled;

    @Parameterized.Parameters(name = "diskAccessMode={0},cursor={1}")
    public static Collection<Object[]> params()
    {
        return Arrays.asList(new Object[]{ DiskAccessMode.standard, true },
                             new Object[]{ DiskAccessMode.standard, false },
                             new Object[]{ DiskAccessMode.direct, true },
                             new Object[]{ DiskAccessMode.direct, false });
    }

    private DiskAccessMode originalDiskAccessMode;
    private boolean originalCursorCompactionEnabled;

    @Before
    public void setCompactionParams()
    {
        originalDiskAccessMode = DatabaseDescriptor.getCompactionReadDiskAccessMode();
        originalCursorCompactionEnabled = DatabaseDescriptor.cursorCompactionEnabled();
        DatabaseDescriptor.setCompactionReadDiskAccessMode(compactionReadDiskAccessMode);
        DatabaseDescriptor.setCursorCompactionEnabled(cursorCompactionEnabled);
    }

    @After
    public void restoreCompactionParams()
    {
        DatabaseDescriptor.setCompactionReadDiskAccessMode(originalDiskAccessMode);
        DatabaseDescriptor.setCursorCompactionEnabled(originalCursorCompactionEnabled);
    }

    @AfterClass
    public static void teardown() throws IOException, InterruptedException, ExecutionException
    {
        TestHelper.teardown();
    }
}
