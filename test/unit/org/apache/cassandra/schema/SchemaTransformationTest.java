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

package org.apache.cassandra.schema;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.serialization.Version;

import static org.junit.Assert.assertEquals;


public class SchemaTransformationTest
{
    @Test
    public void serdeLatest() throws IOException
    {
        SchemaTransformation transformation = new SchemaTransformation()
        {
            final String longString = StringUtils.repeat("a", 100000);
            public Keyspaces apply(ClusterMetadata metadata) {return null;}
            public boolean compatibleWith(ClusterMetadata metadata) {return true;}

            @Override
            public String cql()
            {
                return longString;
            }
        };
        try
        {

            long size = SchemaTransformation.serializer.serializedSize(transformation, Version.V8);
            try (DataOutputBuffer out = new DataOutputBuffer((int) size))
            {
                SchemaTransformation.serializer.serialize(transformation, out, Version.V8);
            }
            throw new RuntimeException("serializing long cql string on V9 should fail");
        }
        catch (AssertionError e)
        {
            //ignored
        }

        long size = SchemaTransformation.serializer.serializedSize(transformation, Version.V9);
        try (DataOutputBuffer out = new DataOutputBuffer((int) size))
        {
            SchemaTransformation.serializer.serialize(transformation, out, Version.V9);
            byte [] bytes = out.toByteArray();
            assertEquals(size, bytes.length);
            // can't deserialize the fake schema transformation as it doesn't parse.
            // BTreeFastBuilderContaminationTest creates a huge table which exercises the deser code
        }
    }
}