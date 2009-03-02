/**
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

package org.apache.cassandra.concurrent;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.continuations.Suspendable;


public class ContinuationStage implements IStage
{
    private String name_;
    private ContinuationsExecutor executorService_;
            
    public ContinuationStage(String name, int numThreads)
    {        
        name_ = name;        
        executorService_ = new ContinuationsExecutor( numThreads,
                numThreads,
                Integer.MAX_VALUE,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(),
                new ThreadFactoryImpl(name)
                );        
    }
    
    public String getName() 
    {        
        return name_;
    }
    
    public ExecutorService getInternalThreadPool()
    {
        return executorService_;
    }

    public Future<Object> execute(Callable<Object> callable) {
        return executorService_.submit(callable);
    }
    
    public void execute(Runnable runnable) {
        executorService_.execute(runnable);
    }
    
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit)
    {
        throw new UnsupportedOperationException("This operation is not supported");
    }
    
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
        throw new UnsupportedOperationException("This operation is not supported");
    }
    
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
        throw new UnsupportedOperationException("This operation is not supported");
    }
    
    public void shutdown() {  
        executorService_.shutdownNow(); 
    }
    
    public boolean isShutdown()
    {
        return executorService_.isShutdown();
    }
    
    public long getTaskCount(){
        return (executorService_.getTaskCount() - executorService_.getCompletedTaskCount());
    }
}
