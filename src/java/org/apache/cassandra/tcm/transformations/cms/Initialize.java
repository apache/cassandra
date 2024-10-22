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

import org.apache.cassandra.auth.AuthKeyspace;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.MetaStrategy;
import org.apache.cassandra.repair.autorepair.AutoRepairKeyspace;
import org.apache.cassandra.schema.DistributedSchema;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.SystemDistributedKeyspace;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.tcm.transformations.ForceSnapshot;
import org.apache.cassandra.tracing.TraceKeyspace;

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
        // when upgrading the schema is read from disk and set correctly in baseState, but we still need to
        // make sure that lastModified is bumped, otherwise the keyspace instances are not created
        DistributedSchema initialSchema = new DistributedSchema(next.schema.getKeyspaces().get(SystemDistributedKeyspace.NAME).isEmpty()
                                                                ? setUpDistributedSystemKeyspaces(next)
                                                                : next.schema.getKeyspaces());
        ClusterMetadata.Transformer transformer = next.transformer().with(initialSchema);
        return Transformation.success(transformer, MetaStrategy.affectedRanges(prev));
    }

    public Keyspaces setUpDistributedSystemKeyspaces(ClusterMetadata next)
    {
        Keyspaces keyspaces = next.schema.getKeyspaces();
        return keyspaces.withAddedOrReplaced(Keyspaces.of(TraceKeyspace.metadata(),
                                                          SystemDistributedKeyspace.metadata(),
                                                          AuthKeyspace.metadata()));
    }

    @Override
    public String toString()
    {
        return "Initialize{" +
               "baseState = " + baseState.epoch +
               '}';
    }
}
