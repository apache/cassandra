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

package org.apache.cassandra.service;

import java.util.List;
import java.util.Map;
import javax.management.openmbean.CompositeData;

public interface ActiveRepairServiceMBean
{
    public static final String MBEAN_NAME = "org.apache.cassandra.db:type=RepairService";

    public List<Map<String, String>> getSessions(boolean all, String rangesStr);
    public void failSession(String session, boolean force);

    /** @deprecated See CASSANDRA-15234 */
    @Deprecated(since = "4.1")
    public void setRepairSessionSpaceInMegabytes(int sizeInMegabytes);
    /** @deprecated See CASSANDRA-15234 */
    @Deprecated(since = "4.1")
    public int getRepairSessionSpaceInMegabytes();

    /**
     * @deprecated use setRepairSessionSpaceInMiB instead as it will not throw non-standard exceptions. See CASSANDRA-17668
     */
    @Deprecated(since = "4.1")
    public void setRepairSessionSpaceInMebibytes(int sizeInMebibytes);
    /**
     * @deprecated use getRepairSessionSpaceInMiB instead. See CASSANDRA-17668
     */
    @Deprecated(since = "4.1")
    public int getRepairSessionSpaceInMebibytes();

    public void setRepairSessionSpaceInMiB(int sizeInMebibytes);
    public int getRepairSessionSpaceInMiB();

    public boolean getUseOffheapMerkleTrees();
    public void setUseOffheapMerkleTrees(boolean value);

    public int getRepairPendingCompactionRejectThreshold();
    public void setRepairPendingCompactionRejectThreshold(int value);

    public List<CompositeData> getRepairStats(List<String> schemaArgs, String rangeString);
    public List<CompositeData> getPendingStats(List<String> schemaArgs, String rangeString);
    public List<CompositeData> cleanupPending(List<String> schemaArgs, String rangeString, boolean force);

    /**
     * Each ongoing repair (incremental and non-incremental) is represented by a
     * {@link ActiveRepairService.ParentRepairSession} entry in the {@link ActiveRepairService} cache.
     * Returns the current number of ongoing repairs (the current number of cached entries).
     *
     * @return current size of the internal cache holding {@link ActiveRepairService.ParentRepairSession} instances
     */
    int parentRepairSessionsCount();
    public int getPaxosRepairParallelism();
    public void setPaxosRepairParallelism(int v);
}
