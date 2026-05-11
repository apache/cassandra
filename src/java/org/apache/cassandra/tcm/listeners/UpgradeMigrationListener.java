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

package org.apache.cassandra.tcm.listeners;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.utils.CassandraVersion;

/**
 * For handling changes in Cassandra version.
 * One use case is to react to the initial migration from Gossip based metadata in Cassandra 5.0 and earlier. When
 * a node first transitions to using ClusterMetadataService, this listener will update its gossip state with the new
 * nodeId based hostId and ensure that is propagated.
 *
 * Another use is for evolving distributed system tables, this listener can identify when a new Cassandra version has
 * been deployed across the cluster and provides a hook to take actions such as creating new internal tables etc.
 */
public class UpgradeMigrationListener implements ChangeListener
{
    private static final Logger logger = LoggerFactory.getLogger(UpgradeMigrationListener.class);
    public void notifyPostCommit(ClusterMetadata prev, ClusterMetadata next, boolean fromSnapshot)
    {
        if (prev.epoch.equals(Epoch.UPGRADE_GOSSIP))
        {
            logger.info("Detected upgrade from gossip mode, updating my host id in gossip to {}", next.myNodeId());
            Gossiper.instance.mergeNodeToGossip(next.myNodeId(), next);
            if (Gossiper.instance.getQuarantineDisabled())
                Gossiper.instance.clearQuarantinedEndpoints();
            return;
        }

        CassandraVersion prevMinVersion = prev.directory.clusterMinVersion.cassandraVersion;
        CassandraVersion minVersion = next.directory.clusterMinVersion.cassandraVersion;
        if (prevMinVersion.compareTo(minVersion) == 0 || (prev.epoch.is(Epoch.EMPTY) && fromSnapshot))
        {
            // nothing to do if the min version in the cluster has not changed
            // likewise, we don't need to trigger if applying a snapshot to a previously empty cluster metadata for e.g.
            // when replaying at startup
            logger.debug("Cluster min version has not changed, nothing to do");
        }
    }
}
