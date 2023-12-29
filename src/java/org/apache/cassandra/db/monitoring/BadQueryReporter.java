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

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.annotations.VisibleForTesting;

public abstract class BadQueryReporter implements IBadQueryReporter
{
    //Store different types of bad queries in this queue and limit this queue to fix size.
    static final Map<BadQuery.BadQueryCategory, ConcurrentLinkedQueue<BadQueryTypes>> BAD_QUERY_CATEGORY_QUEUES = new HashMap<>();
    static final Map<BadQuery.BadQueryCategory, AtomicInteger> CURRENT_SAMPLES = new HashMap<>();

    @Override
    public int getStats(BadQuery.BadQueryCategory type)
    {
        return CURRENT_SAMPLES.get(type).get();
    }

    @VisibleForTesting
    public Map<BadQuery.BadQueryCategory, ConcurrentLinkedQueue<BadQueryTypes>> getBadQueryCategoryQueues()
    {
        return Collections.unmodifiableMap(BAD_QUERY_CATEGORY_QUEUES);
    }

    @VisibleForTesting
    public void clearUnsafe(boolean clearVisitedCache)
    {
        Iterator<Map.Entry<BadQuery.BadQueryCategory, ConcurrentLinkedQueue<BadQueryTypes>>> iter = BAD_QUERY_CATEGORY_QUEUES.entrySet().iterator();
        while (iter.hasNext())
        {
            Map.Entry<BadQuery.BadQueryCategory, ConcurrentLinkedQueue<BadQueryTypes>> entry = iter.next();
            if (clearVisitedCache) {
                for (BadQueryTypes badQueryTypes : entry.getValue())
                {
                    badQueryTypes.cleanup();
                }
            }
            entry.getValue().clear();
        }
        Iterator<Map.Entry<BadQuery.BadQueryCategory, AtomicInteger>> iterator =  CURRENT_SAMPLES.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<BadQuery.BadQueryCategory, AtomicInteger> entry = iterator.next();
            entry.getValue().set(0);
        }
    }
}
