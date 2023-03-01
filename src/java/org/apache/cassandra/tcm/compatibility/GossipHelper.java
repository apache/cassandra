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

package org.apache.cassandra.tcm.compatibility;

import java.util.Collections;
import java.util.UUID;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.schema.DistributedSchema;
import org.apache.cassandra.schema.SchemaKeyspace;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.Period;
import org.apache.cassandra.tcm.membership.Directory;

public class GossipHelper
{
    public static void updateSchemaVersionInGossip(UUID version)
    {
        Gossiper.instance.addLocalApplicationState(ApplicationState.SCHEMA,
                                                   StorageService.instance.valueFactory.schema(version));
    }

    public static ClusterMetadata emptyWithSchemaFromSystemTables()
    {
        return new ClusterMetadata(Epoch.UPGRADE_STARTUP,
                                   Period.EMPTY,
                                   true,
                                   DatabaseDescriptor.getPartitioner(),
                                   DistributedSchema.fromSystemTables(SchemaKeyspace.fetchNonSystemKeyspaces()),
                                   Directory.EMPTY,
                                   Collections.emptySet(),
                                   Collections.emptyMap());
    }
}
