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

import org.apache.cassandra.utils.LogUtil;
import org.apache.log4j.Logger;
import org.apache.cassandra.utils.*;

/**
 * This is a wrapper class for the <i>ScheduledThreadPoolExecutor</i>. It provides an implementation
 * for the <i>afterExecute()</i> found in the <i>ThreadPoolExecutor</i> class to log any unexpected 
 * Runtime Exceptions.
 * 
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */
public final class DebuggableScheduledThreadPoolExecutor extends ScheduledThreadPoolExecutor
{
    private static Logger logger_ = Logger.getLogger(DebuggableScheduledThreadPoolExecutor.class);
    
    public DebuggableScheduledThreadPoolExecutor(int threads,
            ThreadFactory threadFactory)
    {
        super(threads, threadFactory);        
    }
    
    /**
     *  (non-Javadoc)
     * @see java.util.concurrent.ThreadPoolExecutor#afterExecute(java.lang.Runnable, java.lang.Throwable)
     */
    public void afterExecute(Runnable r, Throwable t)
    {
        super.afterExecute(r,t);
        if ( t != null )
        {  
            Context ctx = ThreadLocalContext.get();
            if ( ctx != null )
            {
                Object object = ctx.get(r.getClass().getName());
                
                if ( object != null )
                {
                    logger_.info("**** In afterExecute() " + t.getClass().getName() + " occured while working with " + object + " ****");
                }
                else
                {
                    logger_.info("**** In afterExecute() " + t.getClass().getName() + " occured ****");
                }
            }
            
            Throwable cause = t.getCause();
            if ( cause != null )
            {
                logger_.info( LogUtil.throwableToString(cause) );
            }
            logger_.info( LogUtil.throwableToString(t) );
        }
    }
}
