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

package org.apache.cassandra.distributed.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Assert;

import org.apache.cassandra.distributed.impl.IInvokableInstance;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.service.ActiveRepairService.ParentRepairStatus;
import org.apache.cassandra.service.StorageService;

public class RepairUtil
{
    private RepairUtil()
    {

    }

    public static Map<String, String> fullRange()
    {
        return new HashMap<>() {{
            put(RepairOption.RANGES_KEY, "0:" + Long.MIN_VALUE);
        }};
    }

    public static Map<String, String> forTables(String... tables)
    {
        return new HashMap<>() {{
            put(RepairOption.RANGES_KEY, "0:" + Long.MIN_VALUE);
            put(RepairOption.COLUMNFAMILIES_KEY, String.join(",", tables));
        }};
    }

    public static Result runRepairAndAwait(IInvokableInstance instance, String keyspace, Map<String, String> option)
    {
        int cmd = instance.callOnInstance(() -> {
            int id = StorageService.instance.repairAsync(keyspace, option);
            Assert.assertFalse("repair return status was 0, expected non-zero return status, 0 indicates repair not submitted", id == 0);
            return id;
        });

        List<String> results = instance.callOnInstance(() -> {
            List<String> status;
            do
            {
                Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
                status = StorageService.instance.getParentRepairStatus(cmd);
            }
            while (status == null || ParentRepairStatus.valueOf(status.get(0)) == ParentRepairStatus.IN_PROGRESS);

            return status;
        });
        return new Result(cmd, ParentRepairStatus.valueOf(results.get(0)), results);
    }

    public static final class Result
    {
        public final int cmd;
        public final ParentRepairStatus status;
        public final List<String> fullStatus;

        public Result(int cmd, ParentRepairStatus status, List<String> fullStatus)
        {
            this.cmd = cmd;
            this.status = status;
            this.fullStatus = fullStatus;
        }
    }
}
