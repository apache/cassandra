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
package org.apache.cassandra.repair;

import java.util.List;

import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.Future;

public class NormalRepairTask extends AbstractRepairTask
{
    private final TimeUUID parentSession;
    private final List<CommonRange> commonRanges;
    private final String[] cfnames;

    protected NormalRepairTask(RepairCoordinator coordinator,
                               TimeUUID parentSession,
                               List<CommonRange> commonRanges,
                               String[] cfnames)
    {
        super(coordinator);
        this.parentSession = parentSession;
        this.commonRanges = commonRanges;
        this.cfnames = cfnames;
    }

    @Override
    public String name()
    {
        return "Repair";
    }

    @Override
    public Future<CoordinatedRepairResult> performUnsafe(ExecutorPlus executor)
    {
        return runRepair(parentSession, false, executor, commonRanges, cfnames);
    }
}
