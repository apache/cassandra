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

package org.apache.cassandra.db.commitlog;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.KeyspaceParams;
import org.mockito.Mockito;

public class CommitLogApiTest
{
    @BeforeClass
    public static void beforeClass() throws ConfigurationException
    {
        // Disable durable writes for system keyspaces to prevent system mutations, e.g. sstable_activity,
        // to end up in CL segments and cause unexpected results in this test wrt counting CL segments,
        // see CASSANDRA-12854
        KeyspaceParams.DEFAULT_LOCAL_DURABLE_WRITES = false;
        SchemaLoader.prepareServer();
    }

    @Before
    public void before() throws IOException
    {
        CommitLog.instance.resetUnsafe(true);
    }

    @Test
    public void testForPath()
    {
        AbstractCommitLogSegmentManager original = CommitLog.instance.getSegmentManager();
        File location = Mockito.mock(File.class);
        CommitLog.instance.forPath(location);
        Assert.assertNotEquals(original, CommitLog.instance.getSegmentManager());
        Assert.assertEquals(location, CommitLog.instance.getSegmentManager().storageDirectory);
    }
}
