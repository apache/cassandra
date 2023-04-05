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

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

/**
 * SSTable metadata component used only for validating SSTable.
 *
 * This part is read before opening main Data.db file for validation
 * and discarded immediately after that.
 */
public class ValidationMetadata extends MetadataComponent
{
    public static final IMetadataComponentSerializer serializer = new ValidationMetadataSerializer();

    public final String partitioner;
    public final double bloomFilterFPChance;

    public ValidationMetadata(String partitioner, double bloomFilterFPChance)
    {
        this.partitioner = partitioner;
        this.bloomFilterFPChance = bloomFilterFPChance;
    }

    public MetadataType getType()
    {
        return MetadataType.VALIDATION;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ValidationMetadata that = (ValidationMetadata) o;
        return Double.compare(that.bloomFilterFPChance, bloomFilterFPChance) == 0 && partitioner.equals(that.partitioner);
    }

    @Override
    public int hashCode()
    {
        int result;
        long temp;
        result = partitioner.hashCode();
        temp = Double.doubleToLongBits(bloomFilterFPChance);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    public static class ValidationMetadataSerializer implements IMetadataComponentSerializer<ValidationMetadata>
    {
        public int serializedSize(Version version, ValidationMetadata component) throws IOException
        {
            return TypeSizes.sizeof(component.partitioner) + 8;
        }

        public void serialize(Version version, ValidationMetadata component, DataOutputPlus out) throws IOException
        {
            out.writeUTF(component.partitioner);
            out.writeDouble(component.bloomFilterFPChance);
        }

        public ValidationMetadata deserialize(Version version, DataInputPlus in) throws IOException
        {

            return new ValidationMetadata(in.readUTF(), in.readDouble());
        }
    }
}
