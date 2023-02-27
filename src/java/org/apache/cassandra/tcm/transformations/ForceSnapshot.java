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

package org.apache.cassandra.tcm.transformations;

import java.io.IOException;

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.MetadataKeys;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

/**
 * Inferred ForceSnapshot transformation. When we receive LogState, it may happen that we receive a base state,
 * in which case should apply snapshot, even if we skip multiple epochs while applying it. This transformation
 * is not getting appended into base table, and is only used for forcing state that does not immediately
 * supercede the current one.
 */
public class ForceSnapshot implements Transformation
{
    protected final ClusterMetadata baseState;

    public ForceSnapshot(ClusterMetadata baseState)
    {
        this.baseState = baseState;
    }

    public Kind kind()
    {
        return Kind.FORCE_SNAPSHOT;
    }

    public Result execute(ClusterMetadata clusterMetadata)
    {
        return new Success(baseState, MetadataKeys.CORE_METADATA);
    }

    public String toString()
    {
        return "ForceSnapshot{" +
               "epoch=" + baseState.epoch +
               '}';
    }

    public static final AsymmetricMetadataSerializer<Transformation, ForceSnapshot> serializer = new AsymmetricMetadataSerializer<Transformation, ForceSnapshot>()
    {
        public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
        {
            ClusterMetadata.serializer.serialize(((ForceSnapshot) t).baseState, out, version);
        }

        public ForceSnapshot deserialize(DataInputPlus in, Version version) throws IOException
        {
            return new ForceSnapshot(ClusterMetadata.serializer.deserialize(in, version));
        }

        public long serializedSize(Transformation t, Version version)
        {
            return ClusterMetadata.serializer.serializedSize(((ForceSnapshot) t).baseState, version);
        }
    };
}