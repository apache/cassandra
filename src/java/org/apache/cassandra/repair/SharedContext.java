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

package org.apache.cassandra.repair;

import java.util.Random;
import java.util.function.Supplier;

import org.apache.cassandra.concurrent.ExecutorFactory;
import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.ICompactionManager;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.gms.IGossiper;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessageDelivery;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.streaming.StreamPlan;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MBeanWrapper;

/**
 * Access methods to shared resources and services.
 * <p>
 * In many parts of the code base we reach into the global space to pull out singletons, but this makes testing much harder; the main goals for this type is to make users easier to test.
 *
 * See {@link Global#instance} for the main production path
 */
public interface SharedContext
{
    InetAddressAndPort broadcastAddressAndPort();
    Supplier<Random> random();
    Clock clock();
    ExecutorFactory executorFactory();
    MBeanWrapper mbean();
    ScheduledExecutorPlus optionalTasks();

    MessageDelivery messaging();
    IFailureDetector failureDetector();
    IEndpointSnitch snitch();
    IGossiper gossiper();
    ICompactionManager compactionManager();
    ActiveRepairService repair();
    IValidationManager validationManager();
    TableRepairManager repairManager(ColumnFamilyStore store);
    StreamExecutor streamExecutor();

    class Global implements SharedContext
    {
        public static final Global instance = new Global();

        @Override
        public InetAddressAndPort broadcastAddressAndPort()
        {
            return FBUtilities.getBroadcastAddressAndPort();
        }

        @Override
        public Supplier<Random> random()
        {
            return Random::new;
        }

        @Override
        public Clock clock()
        {
            return Clock.Global.clock();
        }

        @Override
        public ExecutorFactory executorFactory()
        {
            return ExecutorFactory.Global.executorFactory();
        }

        @Override
        public MBeanWrapper mbean()
        {
            return MBeanWrapper.instance;
        }

        @Override
        public ScheduledExecutorPlus optionalTasks()
        {
            return ScheduledExecutors.optionalTasks;
        }

        @Override
        public MessageDelivery messaging()
        {
            return MessagingService.instance();
        }

        @Override
        public IFailureDetector failureDetector()
        {
            return FailureDetector.instance;
        }

        @Override
        public IEndpointSnitch snitch()
        {
            return DatabaseDescriptor.getEndpointSnitch();
        }

        @Override
        public IGossiper gossiper()
        {
            return Gossiper.instance;
        }

        @Override
        public ICompactionManager compactionManager()
        {
            return CompactionManager.instance;
        }

        @Override
        public ActiveRepairService repair()
        {
            return ActiveRepairService.instance();
        }

        @Override
        public IValidationManager validationManager()
        {
            return ValidationManager.instance;
        }

        @Override
        public TableRepairManager repairManager(ColumnFamilyStore store)
        {
            return store.getRepairManager();
        }

        @Override
        public StreamExecutor streamExecutor()
        {
            return StreamPlan::execute;
        }
    }
}
