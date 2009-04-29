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
package org.apache.cassandra.db;

import java.io.IOException;

import org.apache.cassandra.io.DataInputBuffer;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.utils.LogUtil;
import org.apache.log4j.Logger;


public class CalloutDeployVerbHandler implements IVerbHandler
{
    private static Logger logger_ = Logger.getLogger(CalloutDeployVerbHandler.class);
    
    public void doVerb(Message message)
    {
        Object[] body = message.getMessageBody();
        byte[] bytes = (byte[])body[0];
        DataInputBuffer bufIn = new DataInputBuffer();
        bufIn.reset(bytes, bytes.length);
        try
        {
            CalloutDeployMessage cdMessage = CalloutDeployMessage.serializer().deserialize(bufIn);
            /* save the callout to callout cache and to disk. */
            CalloutManager.instance().addCallout( cdMessage.getCallout(), cdMessage.getScript() );
        }
        catch ( IOException ex )
        {
            logger_.warn(LogUtil.throwableToString(ex));
        }        
    }
}
