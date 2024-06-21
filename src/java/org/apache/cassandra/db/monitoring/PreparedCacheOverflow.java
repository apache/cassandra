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

package org.apache.cassandra.db.monitoring;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.service.MonitoringService;

public class PreparedCacheOverflow extends BadQueryTypes
{
    private String ossLog;

    public PreparedCacheOverflow(long count)
    {
        super("", "");
        ossLog = String.format("%d prepared statements discarded in the last minute because cache limit reached ({%d} MB)",
                                                               count,
                                                               DatabaseDescriptor.getPreparedStatementsCacheSizeMiB());
    }

    @Override
    public String toString()
    {
        return ossLog;
    }

    @Override
    public String getKey()
    {
        return "";
    }

    @Override
    public String getDetails()
    {
        return ossLog;
    }

    static void checkForPreparedCacheOverflow(long count)
    {
        if (count > 0) {
            BadQuery.report(BadQuery.BadQueryCategory.PREPARED_CACHE_OVERFLOW, new PreparedCacheOverflow(count));
        }
    }
}


