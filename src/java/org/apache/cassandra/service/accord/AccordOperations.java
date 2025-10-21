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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.transformations.AccordMarkStale;
import org.apache.cassandra.tcm.transformations.AccordMarkRejoining;
import org.apache.cassandra.utils.MBeanWrapper;

public class AccordOperations implements AccordOperationsMBean
{
    public static final String MBEAN_OBJECT_NAME = "org.apache.cassandra.service.accord:type=AccordOperations";
    public static final AccordOperations instance = new AccordOperations(ClusterMetadataService.instance());

    private final ClusterMetadataService cms;

    public static void initJmx()
    {
        MBeanWrapper.instance.registerMBean(instance, MBEAN_OBJECT_NAME);
    }

    private AccordOperations(ClusterMetadataService cms)
    {
        this.cms = cms;
    }

    @Override
    public Map<String, String> describe()
    {
        Map<String, String> info = new HashMap<>();
        ClusterMetadata metadata = ClusterMetadata.current();

        info.put("EPOCH", Long.toString(metadata.epoch.getEpoch()));
        String staleReplicas = metadata.accordStaleReplicas.ids().stream().sorted().map(Object::toString).collect(Collectors.joining(","));
        info.put("STALE_REPLICAS", staleReplicas);
        return info;
    }

    @Override
    public void accordMarkStale(List<String> nodeIdStrings)
    {
        Set<NodeId> nodeIds = nodeIdStrings.stream().map(NodeId::fromString).collect(Collectors.toSet());
        cms.commit(new AccordMarkStale(nodeIds));
    }

    @Override
    public void accordMarkRejoining(List<String> nodeIdStrings)
    {
        Set<NodeId> nodeIds = nodeIdStrings.stream().map(NodeId::fromString).collect(Collectors.toSet());
        cms.commit(new AccordMarkRejoining(nodeIds));
    }
}
