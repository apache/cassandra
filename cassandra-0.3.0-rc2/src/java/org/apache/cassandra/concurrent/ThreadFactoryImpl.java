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

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.apache.cassandra.utils.*;

/**
 * This class is an implementation of the <i>ThreadFactory</i> interface. This 
 * is useful to give Java threads meaningful names which is useful when using 
 * a tool like JConsole.
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class ThreadFactoryImpl implements ThreadFactory
{
    protected String id_;
    protected ThreadGroup threadGroup_;
    protected final AtomicInteger threadNbr_ = new AtomicInteger(1);
    
    public ThreadFactoryImpl(String id)
    {
        SecurityManager sm = System.getSecurityManager();
        threadGroup_ = ( sm != null ) ? sm.getThreadGroup() : Thread.currentThread().getThreadGroup();
        id_ = id;
    }    
    
    public Thread newThread(Runnable runnable)
    {        
        String name = id_ + ":" + threadNbr_.getAndIncrement();       
        Thread thread = new Thread(threadGroup_, runnable, name);        
        return thread;
    }
}
