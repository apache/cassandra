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

package org.apache.cassandra.utils;

import java.lang.management.ManagementFactory;

import com.sun.management.ThreadMXBean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThreadStats
{
    private final static Logger logger = LoggerFactory.getLogger(ThreadStats.class);

    private static final ThreadMXBean threadMxBean;
    static
    {
        ThreadMXBean threadMxBeanToInit;
        try
        {
            threadMxBeanToInit = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
            threadMxBeanToInit.setThreadAllocatedMemoryEnabled(true);
        }
        catch (Throwable e)
        {
            threadMxBeanToInit = null;
            logger.debug("Per thread stats are not available: {}", e.getMessage());
        }
        threadMxBean = threadMxBeanToInit;
    }

    public static long getCurrentThreadCpuTimeNano()
    {
        return threadMxBean == null || !threadMxBean.isCurrentThreadCpuTimeSupported()
               ? -1 : threadMxBean.getCurrentThreadCpuTime();
    }

    public static long getCurrentThreadAllocatedBytes()
    {
        return threadMxBean == null || !threadMxBean.isThreadAllocatedMemorySupported()
               ? -1 : threadMxBean.getCurrentThreadAllocatedBytes();
    }
}
