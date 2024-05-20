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

package org.apache.cassandra.tcm;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface CMSOperationsMBean
{
    public void initializeCMS(List<String> ignore);
    public void resumeReconfigureCms();
    public void reconfigureCMS(int rf);
    public void reconfigureCMS(Map<String, Integer> rf);
    public Map<String, List<String>> reconfigureCMSStatus();
    public void cancelReconfigureCms();

    public Map<String, String> describeCMS();
    public void snapshotClusterMetadata();

    public void unsafeRevertClusterMetadata(long epoch);
    public String dumpClusterMetadata(long epoch, long transformToEpoch, String version) throws IOException;
    public String dumpClusterMetadata() throws IOException;
    public void unsafeLoadClusterMetadata(String file) throws IOException;

    public void setCommitsPaused(boolean paused);
    public boolean getCommitsPaused();

    public boolean cancelInProgressSequences(String sequenceOwner, String expectedSequenceKind);

    public void unregisterLeftNodes(List<String> nodeIds);
}
