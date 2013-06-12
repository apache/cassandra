package org.apache.cassandra.service.paxos;
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


import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.net.MessageIn;

public class PrepareCallback extends AbstractPaxosCallback<PrepareResponse>
{
    private static final Logger logger = LoggerFactory.getLogger(PrepareCallback.class);

    public boolean promised = true;
    public Commit mostRecentCommit;
    public Commit inProgressCommit;

    private Map<InetAddress, Commit> commitsByReplica = new HashMap<InetAddress, Commit>();

    public PrepareCallback(ByteBuffer key, CFMetaData metadata, int targets)
    {
        super(targets);
        // need to inject the right key in the empty commit so comparing with empty commits in the reply works as expected
        mostRecentCommit = Commit.emptyCommit(key, metadata);
        inProgressCommit = Commit.emptyCommit(key, metadata);
    }

    public synchronized void response(MessageIn<PrepareResponse> message)
    {
        PrepareResponse response = message.payload;
        logger.debug("Prepare response {} from {}", response, message.from);

        if (!response.promised)
        {
            promised = false;
            while (latch.getCount() > 0)
                latch.countDown();
            return;
        }

        if (response.mostRecentCommit.isAfter(mostRecentCommit))
            mostRecentCommit = response.mostRecentCommit;

        if (response.inProgressCommit.isAfter(inProgressCommit))
            inProgressCommit = response.inProgressCommit;

        latch.countDown();
    }

    public Iterable<InetAddress> replicasMissingMostRecentCommit()
    {
        return Iterables.filter(commitsByReplica.keySet(), new Predicate<InetAddress>()
        {
            public boolean apply(InetAddress inetAddress)
            {
                return (!commitsByReplica.get(inetAddress).ballot.equals(mostRecentCommit.ballot));
            }
        });
    }
}
