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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DefsTable;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.net.IAsyncResult;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.WrappedRunnable;

class MigrationTask extends WrappedRunnable
{
    private static final Logger logger = LoggerFactory.getLogger(MigrationTask.class);

    private final InetAddress endpoint;

    MigrationTask(InetAddress endpoint)
    {
        this.endpoint = endpoint;
    }

    public void runMayThrow() throws Exception
    {
        MessageOut message = new MessageOut(MessagingService.Verb.MIGRATION_REQUEST, null, MigrationManager.MigrationsSerializer.instance);

        int retries = 0;
        while (retries < MigrationManager.MIGRATION_REQUEST_RETRIES)
        {
            if (!FailureDetector.instance.isAlive(endpoint))
            {
                logger.error("Can't send migration request: node {} is down.", endpoint);
                return;
            }

            IAsyncResult<Collection<RowMutation>> iar = MessagingService.instance().sendRR(message, endpoint);
            try
            {
                Collection<RowMutation> schema = iar.get(DatabaseDescriptor.getRpcTimeout(), TimeUnit.MILLISECONDS);
                DefsTable.mergeSchema(schema);
                return;
            }
            catch(TimeoutException e)
            {
                retries++;
            }
        }
    }
}
