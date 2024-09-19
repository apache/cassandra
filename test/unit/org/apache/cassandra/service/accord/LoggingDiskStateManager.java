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

package org.apache.cassandra.service.accord;

import accord.local.Node;
import accord.primitives.Ranges;
import accord.topology.Topology;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * When trying to inspect the order in which disk state is modified, this class can aid by adding logging.  This class
 * mostly exists for testing to aid in debugging.
 */
@SuppressWarnings("unused")
@VisibleForTesting
public class LoggingDiskStateManager implements AccordConfigurationService.DiskStateManager {
    private static final Logger logger = LoggerFactory.getLogger(LoggingDiskStateManager.class);
    private final Node.Id self;
    private final AccordConfigurationService.DiskStateManager delegate;

    public LoggingDiskStateManager(Node.Id self, AccordConfigurationService.DiskStateManager delegate) {
        this.self = self;
        this.delegate = delegate;
    }

    @Override
    public AccordKeyspace.EpochDiskState loadTopologies(AccordKeyspace.TopologyLoadConsumer consumer) {
        logger.info("[node={}] Calling loadTopologies()", self);
        return delegate.loadTopologies(consumer);
    }

    @Override
    public AccordKeyspace.EpochDiskState setNotifyingLocalSync(long epoch, Set<Node.Id> pending, AccordKeyspace.EpochDiskState diskState) {
        logger.info("[node={}] Calling setNotifyingLocalSync({}, {}, {})", self, epoch, pending, diskState);
        return delegate.setNotifyingLocalSync(epoch, pending, diskState);
    }

    @Override
    public AccordKeyspace.EpochDiskState setCompletedLocalSync(long epoch, AccordKeyspace.EpochDiskState diskState) {
        logger.info("[node={}] Calling setCompletedLocalSync({}, {})", self, epoch, diskState);
        return delegate.setCompletedLocalSync(epoch, diskState);
    }

    @Override
    public AccordKeyspace.EpochDiskState markLocalSyncAck(Node.Id id, long epoch, AccordKeyspace.EpochDiskState diskState) {
        logger.info("[node={}] Calling markLocalSyncAck({}, {}, {})", self, id, epoch, diskState);
        return delegate.markLocalSyncAck(id, epoch, diskState);
    }

    @Override
    public AccordKeyspace.EpochDiskState saveTopology(Topology topology, AccordKeyspace.EpochDiskState diskState) {
        logger.info("[node={}] Calling saveTopology({}, {})", self, topology.epoch(), diskState);
        return delegate.saveTopology(topology, diskState);
    }

    @Override
    public AccordKeyspace.EpochDiskState markRemoteTopologySync(Node.Id id, long epoch, AccordKeyspace.EpochDiskState diskState) {
        logger.info("[node={}] Calling markRemoteTopologySync({}, {}, {})", self, id, epoch, diskState);
        return delegate.markRemoteTopologySync(id, epoch, diskState);
    }

    @Override
    public AccordKeyspace.EpochDiskState markClosed(Ranges ranges, long epoch, AccordKeyspace.EpochDiskState diskState) {
        logger.info("[node={}] Calling markClosed({}, {}, {})", self, ranges, epoch, diskState);
        return delegate.markClosed(ranges, epoch, diskState);
    }

    @Override
    public AccordKeyspace.EpochDiskState truncateTopologyUntil(long epoch, AccordKeyspace.EpochDiskState diskState) {
        logger.info("[node={}] Calling truncateTopologyUntil({}, {})", self, epoch, diskState);
        return delegate.truncateTopologyUntil(epoch, diskState);
    }
}
