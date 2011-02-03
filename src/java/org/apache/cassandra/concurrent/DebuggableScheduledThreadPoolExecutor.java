package org.apache.cassandra.concurrent;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import java.util.concurrent.*;

import org.apache.log4j.Logger;

public class DebuggableScheduledThreadPoolExecutor extends ScheduledThreadPoolExecutor
{
    protected static Logger logger = Logger.getLogger(DebuggableScheduledThreadPoolExecutor.class);

    public DebuggableScheduledThreadPoolExecutor(String threadPoolName, int priority)
    {
        this(1, threadPoolName, priority);
    }

    public DebuggableScheduledThreadPoolExecutor(int corePoolSize, String threadPoolName, int priority)
    {
        super(corePoolSize, new NamedThreadFactory(threadPoolName, priority));
    }

    public DebuggableScheduledThreadPoolExecutor(String threadPoolName)
    {
        this(1, threadPoolName, Thread.NORM_PRIORITY);
    }

    @Override
    public void afterExecute(Runnable r, Throwable t)
    {
        super.afterExecute(r,t);
        DebuggableThreadPoolExecutor.logTaskException(r, t);
    }
}
