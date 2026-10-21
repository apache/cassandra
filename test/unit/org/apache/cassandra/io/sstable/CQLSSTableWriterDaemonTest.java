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

import java.util.Arrays;
import java.util.Collection;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.Config.DiskAccessMode;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;

@RunWith(Parameterized.class)
public class CQLSSTableWriterDaemonTest extends CQLSSTableWriterTest
{
    @Parameterized.Parameter
    public DiskAccessMode backgroundWriteMode;

    @Parameterized.Parameters(name = "backgroundWriteMode={0}")
    public static Collection<Object[]> modes()
    {
        return Arrays.asList(new Object[]{ DiskAccessMode.standard },
                             new Object[]{ DiskAccessMode.direct });
    }

    private DiskAccessMode originalBackgroundWriteMode;

    @BeforeClass
    public static void setup() throws Exception
    {
        DatabaseDescriptor.daemonInitialization();
        CommitLog.instance.start();
        SchemaLoader.cleanupAndLeaveDirs();
        Keyspace.setInitialized();
        ServerTestUtils.prepareServerNoRegister();
        MessagingService.instance().waitUntilListeningUnchecked();
        StorageService.instance.initServer();
    }

    @Before
    public void setBackgroundWriteMode()
    {
        originalBackgroundWriteMode = DatabaseDescriptor.getBackgroundWriteDiskAccessMode();
        DatabaseDescriptor.setBackgroundWriteDiskAccessMode(backgroundWriteMode);
    }

    @After
    public void restoreBackgroundWriteMode()
    {
        DatabaseDescriptor.setBackgroundWriteDiskAccessMode(originalBackgroundWriteMode);
    }
}
