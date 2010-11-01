/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.        See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.        The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.        You may obtain a copy of the License at
 * 
 *         http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.        See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.hadoop.streaming;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.hadoop.streaming.io.IdentifierResolver;

/**
 * Resolves AVRO_ID to the appropriate OutputReader and K/V classes for Cassandra output.
 *
 * TODO: usage explanation
 */
public class AvroResolver extends IdentifierResolver
{
    public static final String AVRO_ID = "cassandra_avro_output";

    @Override
    public void resolve(String identifier)
    {
        if (!identifier.equalsIgnoreCase(AVRO_ID))
        {
            super.resolve(identifier);
            return;
        }

        setInputWriterClass(null);
        setOutputReaderClass(AvroOutputReader.class);
        setOutputKeyClass(ByteBuffer.class);
        setOutputValueClass(List.class);
    }
}
