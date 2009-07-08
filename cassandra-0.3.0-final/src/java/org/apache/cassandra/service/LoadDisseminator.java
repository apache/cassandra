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
package org.apache.cassandra.service;

import java.util.TimerTask;

import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.utils.FileUtils;
import org.apache.cassandra.utils.LogUtil;
import org.apache.log4j.Logger;

class LoadDisseminator extends TimerTask
{
    private final static Logger logger_ = Logger.getLogger(LoadDisseminator.class);
    protected final static String loadInfo_= "LOAD-INFORMATION";
    
    public void run()
    {
        try
        {
            long diskSpace = FileUtils.getUsedDiskSpace();                
            String diskUtilization = FileUtils.stringifyFileSize(diskSpace);
            logger_.debug("Disseminating load info ...");
            Gossiper.instance().addApplicationState(LoadDisseminator.loadInfo_, new ApplicationState(diskUtilization));
        }
        catch ( Throwable ex )
        {
            logger_.warn( LogUtil.throwableToString(ex) );
        }
    }
}
