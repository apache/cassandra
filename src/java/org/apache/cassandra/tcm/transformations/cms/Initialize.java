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

package org.apache.cassandra.tcm.transformations.cms;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.auth.AuthKeyspace;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.DistributedSchema;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.SchemaTransformation;
import org.apache.cassandra.schema.SchemaTransformations;
import org.apache.cassandra.schema.SystemDistributedKeyspace;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.tcm.transformations.ForceSnapshot;
import org.apache.cassandra.tracing.TraceKeyspace;

import static org.apache.cassandra.tcm.transformations.cms.EntireRange.affectedRanges;

public class Initialize extends ForceSnapshot
{
    public static final AsymmetricMetadataSerializer<Transformation, Initialize> serializer = new AsymmetricMetadataSerializer<Transformation, Initialize>()
    {
        public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
        {
            Initialize initialize = (Initialize) t;
            ClusterMetadata.serializer.serialize(initialize.baseState, out, version);
        }

        public Initialize deserialize(DataInputPlus in, Version version) throws IOException
        {
            return new Initialize(ClusterMetadata.serializer.deserialize(in, version));
        }

        public long serializedSize(Transformation t, Version version)
        {
            Initialize initialize = (Initialize) t;
            return ClusterMetadata.serializer.serializedSize(initialize.baseState, version);
        }
    };

    public Initialize(ClusterMetadata baseState)
    {
        super(baseState);
    }

    public Kind kind()
    {
        return Kind.INITIALIZE_CMS;
    }

    public Result execute(ClusterMetadata prev)
    {
        ClusterMetadata next = baseState;
        DistributedSchema initialSchema = new DistributedSchema(setUpDistributedSystemKeyspaces(next));
        ClusterMetadata.Transformer transformer = next.transformer().with(initialSchema);
        return success(transformer, affectedRanges);
    }

    public static final List<SchemaTransformation> schemaTransformations =
    Collections.unmodifiableList(Arrays.asList(SchemaTransformations.fromCql(String.format("CREATE KEYSPACE IF NOT EXISTS %s " +
                                                                                           "  WITH REPLICATION = { \n" +
                                                                                           "   'class' : 'SimpleStrategy', \n" +
                                                                                           "   'replication_factor' : %d \n" +
                                                                                           "  };",
                                                                                           SchemaConstants.TRACE_KEYSPACE_NAME,
                                                                                           Math.max(TraceKeyspace.DEFAULT_RF, DatabaseDescriptor.getDefaultKeyspaceRF()))),
                                               SchemaTransformations.fromCql(String.format(TraceKeyspace.SESSIONS_CQL, SchemaConstants.TRACE_KEYSPACE_NAME + "." + TraceKeyspace.SESSIONS)),
                                               SchemaTransformations.fromCql(String.format(TraceKeyspace.EVENTS_CQL, SchemaConstants.TRACE_KEYSPACE_NAME + "." + TraceKeyspace.EVENTS)),
                                               SchemaTransformations.fromCql(String.format("CREATE KEYSPACE IF NOT EXISTS %s " +
                                                                                           "  WITH REPLICATION = { \n" +
                                                                                           "   'class' : 'SimpleStrategy', \n" +
                                                                                           "   'replication_factor' : %d \n" +
                                                                                           "  };",
                                                                                           SchemaConstants.DISTRIBUTED_KEYSPACE_NAME,
                                                                                           Math.max(SystemDistributedKeyspace.DEFAULT_RF, DatabaseDescriptor.getDefaultKeyspaceRF()))),
                                               SchemaTransformations.fromCql(String.format(SystemDistributedKeyspace.REPAIR_HISTORY_CQL, SchemaConstants.DISTRIBUTED_KEYSPACE_NAME + "." + SystemDistributedKeyspace.REPAIR_HISTORY)),
                                               SchemaTransformations.fromCql(String.format(SystemDistributedKeyspace.PARENT_REPAIR_HISTORY_CQL, SchemaConstants.DISTRIBUTED_KEYSPACE_NAME + "." + SystemDistributedKeyspace.PARENT_REPAIR_HISTORY)),
                                               SchemaTransformations.fromCql(String.format(SystemDistributedKeyspace.VIEW_BUILD_STATUS_CQL, SchemaConstants.DISTRIBUTED_KEYSPACE_NAME + "." + SystemDistributedKeyspace.VIEW_BUILD_STATUS)),
                                               SchemaTransformations.fromCql(String.format("CREATE KEYSPACE IF NOT EXISTS %s " +
                                                                                           "  WITH REPLICATION = { \n" +
                                                                                           "   'class' : 'SimpleStrategy', \n" +
                                                                                           "   'replication_factor' : %d \n" +
                                                                                           "  };",
                                                                                           SchemaConstants.AUTH_KEYSPACE_NAME,
                                                                                           Math.max(AuthKeyspace.DEFAULT_RF, DatabaseDescriptor.getDefaultKeyspaceRF()))),
                                               SchemaTransformations.fromCql(String.format(AuthKeyspace.ROLES_CQL, SchemaConstants.AUTH_KEYSPACE_NAME + "." + AuthKeyspace.ROLES)),
                                               SchemaTransformations.fromCql(String.format(AuthKeyspace.ROLE_MEMBERS_CQL, SchemaConstants.AUTH_KEYSPACE_NAME + "." + AuthKeyspace.ROLE_MEMBERS)),
                                               SchemaTransformations.fromCql(String.format(AuthKeyspace.ROLE_PERMISSIONS_CQL, SchemaConstants.AUTH_KEYSPACE_NAME + "." + AuthKeyspace.ROLE_PERMISSIONS)),
                                               SchemaTransformations.fromCql(String.format(AuthKeyspace.RESOURCE_ROLE_INDEX_CQL, SchemaConstants.AUTH_KEYSPACE_NAME + "." + AuthKeyspace.RESOURCE_ROLE_INDEX)),
                                               SchemaTransformations.fromCql(String.format(AuthKeyspace.NETWORK_PERMISSIONS_CQL, SchemaConstants.AUTH_KEYSPACE_NAME + "." + AuthKeyspace.NETWORK_PERMISSIONS))));

    public Keyspaces setUpDistributedSystemKeyspaces(ClusterMetadata metadata)
    {
        Keyspaces keyspaces = metadata.schema.getKeyspaces();

        for (SchemaTransformation transformation : schemaTransformations)
            keyspaces = transformation.apply(metadata, keyspaces);

        return keyspaces;
    }

    @Override
    public String toString()
    {
        return "Initialize{" +
               "baseState = " + baseState +
               '}';
    }
}
