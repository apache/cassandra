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

package org.apache.cassandra.net;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.StorageService;

public class MessageDeliveryTask implements Runnable
{
    private static final Logger logger = LoggerFactory.getLogger(MessageDeliveryTask.class);

    private Message message;
    private final long constructionTime = System.currentTimeMillis();
    private final String id;

    public MessageDeliveryTask(Message message, String id)
    {
        assert message != null;
        this.message = message;
        this.id = id;
    }

    public void run()
    {
        StorageService.Verb verb = message.getVerb();
        if (MessagingService.DROPPABLE_VERBS.contains(verb)
            && System.currentTimeMillis() > constructionTime + DatabaseDescriptor.getRpcTimeout())
        {
            MessagingService.instance().incrementDroppedMessages(verb);
            return;
        }

        IVerbHandler verbHandler = MessagingService.instance().getVerbHandler(verb);
        if (verbHandler == null)
        {
            logger.debug("Unknown verb {}", verb);
            return;
        }

        verbHandler.doVerb(message, id);
    }
}
