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

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.exceptions.ExceptionCode;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.sequences.LockedRanges;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

// TODO (expected, interop): improve mechanism for adding tables (probably want table level granularity, option to not-auto-add, and option to remove)
public class AddAccordKeyspace implements Transformation
{
    private final String keyspace;

    public AddAccordKeyspace(String keyspace)
    {
        this.keyspace = keyspace;
    }

    public Kind kind()
    {
        return Kind.ADD_ACCORD_KEYSPACE;
    }

    public Result execute(ClusterMetadata metadata)
    {
        if (metadata.accordKeyspaces.contains(keyspace))
            return new Rejected(ExceptionCode.ALREADY_EXISTS, keyspace + " is already an accord keyspaces");

        return Transformation.success(metadata.transformer().withAccordKeyspace(keyspace), LockedRanges.AffectedRanges.EMPTY);
    }

    public static final AsymmetricMetadataSerializer<Transformation, AddAccordKeyspace> serializer = new AsymmetricMetadataSerializer<Transformation, AddAccordKeyspace>()
    {
        public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
        {
            assert t instanceof AddAccordKeyspace;
            AddAccordKeyspace addKeyspace = (AddAccordKeyspace) t;
            out.writeUTF(addKeyspace.keyspace);
        }

        public AddAccordKeyspace deserialize(DataInputPlus in, Version version) throws IOException
        {
            return new AddAccordKeyspace(in.readUTF());
        }

        public long serializedSize(Transformation t, Version version)
        {
            assert t instanceof AddAccordKeyspace;
            AddAccordKeyspace addKeyspace = (AddAccordKeyspace) t;
            return TypeSizes.sizeof(addKeyspace.keyspace);
        }
    };
}
