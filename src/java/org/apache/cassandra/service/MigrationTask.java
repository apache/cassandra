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
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.SystemKeyspace.BootstrapState;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.net.IAsyncCallbackWithFailure;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.SchemaKeyspace;
import org.apache.cassandra.utils.WrappedRunnable;


public final class MigrationTask extends WrappedRunnable
{
    private static final Logger logger = LoggerFactory.getLogger(MigrationTask.class);

    private static final Comparator<InetAddress> inetcomparator = new Comparator<InetAddress>()
    {
        public int compare(InetAddress addr1, InetAddress addr2)
        {
            return addr1.getHostAddress().compareTo(addr2.getHostAddress());
        }
    };

    public static Multimap<UUID, InetAddress> schemaNodesMap = Multimaps.synchronizedMultimap(Multimaps.newSetMultimap(new ConcurrentHashMap<>(), HashSet::new));
    private static final Set<InetAddress> finished = new ConcurrentSkipListSet<>(inetcomparator);
    private static final Set<InetAddress> submitted = new ConcurrentSkipListSet<>(inetcomparator);

    public static Set<InetAddress> getSubmitted()
    {
        return new HashSet<>(submitted);
    }

    public static Set<InetAddress> getFinished()
    {
        return new HashSet<>(finished);
    }

    public static void removeFinished(Set<InetAddress> finished)
    {
        MigrationTask.finished.removeAll(finished);
    }

    public static void reset()
    {
        schemaNodesMap.clear();
        finished.clear();
        submitted.clear();
    }

    private final InetAddress endpoint;

    MigrationTask(InetAddress endpoint)
    {
        this.endpoint = endpoint;
    }

    public void runMayThrow()
    {
        if (!FailureDetector.instance.isAlive(endpoint))
        {
            logger.warn("Can't send schema pull request: node {} is down.", endpoint);
            return;
        }

        // There is a chance that quite some time could have passed between now and the MM#maybeScheduleSchemaPull(),
        // potentially enough for the endpoint node to restart - which is an issue if it does restart upgraded, with
        // a higher major.
        if (!MigrationManager.shouldPullSchemaFrom(endpoint))
        {
            logger.info("Skipped sending a migration request: node {} has a higher major version now.", endpoint);
            return;
        }

        MessageOut<?> message = new MessageOut<>(MessagingService.Verb.MIGRATION_REQUEST, null, MigrationManager.MigrationsSerializer.instance);

        logger.info("Sending schema pull request to {} at {} with timeout {}", endpoint, System.currentTimeMillis(), message.getTimeout());

        submitted.add(endpoint);

        MessagingService.instance().sendRR(message, endpoint, new MigrationTaskCallback(), message.getTimeout(), true);
    }

    public static class MigrationTaskCallback implements IAsyncCallbackWithFailure<Collection<Mutation>>
    {
        private static final Logger logger = LoggerFactory.getLogger(MigrationTaskCallback.class);

        @Override
        public void response(MessageIn<Collection<Mutation>> message)
        {
            try
            {
                logger.info("Received response to schema request from {} at {}", message.from, System.currentTimeMillis());

                SchemaKeyspace.mergeSchemaAndAnnounceVersion(message.payload);

                // add me among finished requests, this will be reached only in case that
                // above merging of schema went without throwing any exception
                // we do not know what schema version this particular message was responsible for
                // and we do not really care anyway here
                finished.add(message.from);
            }
            catch (ConfigurationException e)
            {
                logger.error("Configuration exception merging remote schema from " + message.from, e);
            }
            finally
            {
                // remove me from set of submitted messages
                // so we know if we should skip sending if it is in progress
                // or we should indeed send a message to that node
                submitted.remove(message.from);
            }
        }

        public void onFailure(InetAddress from, RequestFailureReason failureReason)
        {
            // remove ne from set of submitted requests,
            // this will be reached either when a proper failure occurs or when this callback expires
            // if it is not among submitted, it is not among finished either,
            // it can be not in submitted but in finished only on happy path
            submitted.remove(from);
        }

        public boolean isLatencyForSnitch()
        {
            return false;
        }
    }
}
