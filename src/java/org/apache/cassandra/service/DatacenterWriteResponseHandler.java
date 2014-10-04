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
package org.apache.cassandra.service;

import java.net.InetAddress;
import java.util.Collection;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.WriteType;

/**
 * This class blocks for a quorum of responses _in the local datacenter only_ (CL.LOCAL_QUORUM and CL.LOCAL_ONE).
 */
public class DatacenterWriteResponseHandler extends WriteResponseHandler
{
    private static final IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
    
    public DatacenterWriteResponseHandler(Collection<InetAddress> naturalEndpoints,
                                          Collection<InetAddress> pendingEndpoints,
                                          ConsistencyLevel consistencyLevel,
                                          Table table,
                                          Runnable callback,
                                          WriteType writeType)
    {
        super(naturalEndpoints, pendingEndpoints, consistencyLevel, table, callback, writeType);
        assert consistencyLevel.isDatacenterLocal();
    }

    @Override
    public void response(MessageIn message)
    {
        if (message == null || isLocal(message.from))
        {
            if (responses.decrementAndGet() == 0)
                signal();
        }
    }
    
	@Override
	protected int totalBlockFor() 
	{
		// during bootstrap, include pending endpoints (only local here) in the count
		// or we may fail the consistency level guarantees (see #833)
		return consistencyLevel.blockFor(table) + countPendingEndPoints();
	}

	private int countPendingEndPoints()
    {
		int count = 0;
		
		// filter only local pending endpoints
        for ( InetAddress pending : this.pendingEndpoints )
        {
        	if ( isLocal(pending) ) 
        		count++;
        }
        
        return count;
	}
	
	private boolean isLocal(InetAddress ad)
	{
		return DatabaseDescriptor.getLocalDataCenter().equals(snitch.getDatacenter(ad));
	}
}
