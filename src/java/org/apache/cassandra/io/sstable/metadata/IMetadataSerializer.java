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
package org.apache.cassandra.io.sstable.metadata;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;

import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.DataOutputPlus;

/**
 * Interface for SSTable metadata serializer
 */
public interface IMetadataSerializer
{
    /**
     * Serialize given metadata components
     *
     *
     * @param components Metadata components to serialize
     * @param out
     * @param version
     * @throws IOException
     */
    void serialize(Map<MetadataType, MetadataComponent> components, DataOutputPlus out, Version version) throws IOException;

    /**
     * Deserialize specified metadata components from given descriptor.
     *
     * @param descriptor SSTable descriptor
     * @return Deserialized metadata components, in deserialized order.
     * @throws IOException
     */
    Map<MetadataType, MetadataComponent> deserialize(Descriptor descriptor, EnumSet<MetadataType> types) throws IOException;

    /**
     * Deserialized only metadata component specified from given descriptor.
     *
     * @param descriptor SSTable descriptor
     * @param type Metadata component type to deserialize
     * @return Deserialized metadata component. Can be null if specified type does not exist.
     * @throws IOException
     */
    MetadataComponent deserialize(Descriptor descriptor, MetadataType type) throws IOException;

    /**
     * Mutate SSTable level
     *
     * @param descriptor SSTable descriptor
     * @param newLevel new SSTable level
     * @throws IOException
     */
    void mutateLevel(Descriptor descriptor, int newLevel) throws IOException;

    /**
     * Mutate repairedAt time
     */
    void mutateRepairedAt(Descriptor descriptor, long newRepairedAt) throws IOException;

    /**
     * Replace the sstable metadata file ({@code -Statistics.db}) with the given components.
     */
    void rewriteSSTableMetadata(Descriptor descriptor, Map<MetadataType, MetadataComponent> currentComponents) throws IOException;
}
