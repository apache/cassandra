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
package org.apache.cassandra.schema;

import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;

/**
 * Called when node receives updated schema state from the schema migration coordinator node.
 * Such happens when user makes local schema migration on one of the nodes in the ring
 * (which is going to act as coordinator) and that node sends (pushes) it's updated schema state
 * (in form of mutations) to all the alive nodes in the cluster.
 */
public final class SchemaPushVerbHandler implements IVerbHandler<Collection<Mutation>>
{
    private static final Logger logger = LoggerFactory.getLogger(SchemaPushVerbHandler.class);

    public void doVerb(final MessageIn<Collection<Mutation>> message, int id)
    {
        logger.trace("Received schema push request from {}", message.from);

        StageManager.getStage(Stage.MIGRATION).submit(() -> Schema.instance.mergeAndAnnounceVersion(message.payload));
    }
}