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
package org.apache.cassandra.streaming;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import org.junit.Test;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.utils.FBUtilities;

public class SessionInfoTest
{
    /**
     * Test if total numbers are collect
     */
    @Test
    public void testTotals()
    {
        TableId tableId = TableId.generate();
        InetAddressAndPort local = FBUtilities.getLocalAddressAndPort();

        Collection<StreamSummary> summaries = new ArrayList<>();
        for (int i = 0; i < 10; i++)
        {
            StreamSummary summary = new StreamSummary(tableId, i, (i + 1) * 10);
            summaries.add(summary);
        }

        StreamSummary sending = new StreamSummary(tableId, 10, 100);
        SessionInfo info = new SessionInfo(local, 0, local, summaries, Collections.singleton(sending), StreamSession.State.PREPARING, null);

        assert info.getTotalFilesToReceive() == 45;
        assert info.getTotalFilesToSend() == 10;
        assert info.getTotalSizeToReceive() == 550;
        assert info.getTotalSizeToSend() == 100;
        // still, no files received or sent
        assert info.getTotalFilesReceived() == 0;
        assert info.getTotalFilesSent() == 0;

        // receive in progress
        info.updateProgress(new ProgressInfo(local, 0, "test.txt", ProgressInfo.Direction.IN, 50, 50, 100));
        // still in progress, but not completed yet
        assert info.getTotalSizeReceived() == 50;
        assert info.getTotalSizeSent() == 0;
        assert info.getTotalFilesReceived() == 0;
        assert info.getTotalFilesSent() == 0;
        info.updateProgress(new ProgressInfo(local, 0, "test.txt", ProgressInfo.Direction.IN, 100, 100, 100));
        // 1 file should be completed
        assert info.getTotalSizeReceived() == 100;
        assert info.getTotalSizeSent() == 0;
        assert info.getTotalFilesReceived() == 1;
        assert info.getTotalFilesSent() == 0;
    }
}
