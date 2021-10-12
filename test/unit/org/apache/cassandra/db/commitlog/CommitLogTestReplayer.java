/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.db.commitlog;

import org.apache.cassandra.io.util.File;
import java.io.IOException;

import com.google.common.base.Predicate;
import org.junit.Assert;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.RebufferingInputStream;

/**
 * Utility class for tests needing to examine the commitlog contents.
 */
public class CommitLogTestReplayer extends CommitLogReplayer
{
    private final Predicate<Mutation> processor;

    public CommitLogTestReplayer(Predicate<Mutation> processor) throws IOException
    {
        super(CommitLog.instance, CommitLogPosition.NONE, null, ReplayFilter.create());
        CommitLog.instance.sync(true);

        this.processor = processor;
        commitLogReader = new CommitLogTestReader();
    }

    public void examineCommitLog() throws IOException
    {
        replayFiles(new File(DatabaseDescriptor.getCommitLogLocation()).tryList());
    }

    private class CommitLogTestReader extends CommitLogReader
    {
        @Override
        protected void readMutation(CommitLogReadHandler handler,
                                    byte[] inputBuffer,
                                    int size,
                                    CommitLogPosition minPosition,
                                    final int entryLocation,
                                    final CommitLogDescriptor desc) throws IOException
        {
            RebufferingInputStream bufIn = new DataInputBuffer(inputBuffer, 0, size);
            Mutation mutation;
            try
            {
                mutation = Mutation.serializer.deserialize(bufIn, desc.getMessagingVersion(), DeserializationHelper.Flag.LOCAL);
                Assert.assertTrue(processor.apply(mutation));
            }
            catch (IOException e)
            {
                // Test fails.
                throw new AssertionError(e);
            }
        }
    }
}
