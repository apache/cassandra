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

package org.apache.cassandra.tcm.ownership;

//import java.io.IOException;
//import java.util.*;
//
//import org.apache.cassandra.db.TypeSizes;
//import org.apache.cassandra.io.IVersionedSerializer;
//import org.apache.cassandra.io.util.DataInputPlus;
//import org.apache.cassandra.io.util.DataOutputPlus;
//import org.apache.cassandra.schema.ReplicationParams;
//
//public class GlobalPlacementDelta
//{
//    public static final Serializer serializer = new Serializer();
//    public final PlacementDeltas readDeltas;
//    public final PlacementDeltas writeDeltas;
//
//    private GlobalPlacementDelta(PlacementDeltas readDeltas, PlacementDeltas writeDeltas)
//    {
//        this.readDeltas = readDeltas;
//        this.writeDeltas = writeDeltas;
//    }
//
//    public DataPlacements apply(DataPlacements placements)
//    {
//        return apply(placements, readDeltas)
//        return DataPlacements.builder()
//                             .reads(apply(placements.reads, readDeltas))
//                             .writes(apply(placements.writes, writeDeltas))
//                             .build();
//    }
//
//    public static DataPlacements apply(DataPlacements before, PlacementDeltas delta)
//    {
//        DataPlacements.Builder mapBuilder = DataPlacements.builder(before.size());
//        before.asMap().forEach((params, placement) -> {
//            PlacementDeltas.PlacementDelta d = delta.get(params);
//            if (d == null)
//                mapBuilder.put(params, placement);
//            else
//                mapBuilder.put(params, d.apply(placement));
//        });
//        return mapBuilder.build();
//    }
//
//    public boolean equals(Object o)
//    {
//        if (this == o) return true;
//        if (o == null || getClass() != o.getClass()) return false;
//        GlobalPlacementDelta that = (GlobalPlacementDelta) o;
//        return Objects.equals(readDeltas, that.readDeltas) && Objects.equals(writeDeltas, that.writeDeltas);
//    }
//
//    public int hashCode()
//    {
//        return Objects.hash(readDeltas, writeDeltas);
//    }
//
//    public String toString()
//    {
//        return "PlacementDelta{" +
//               "reads=" + readDeltas +
//               ", writes=" + writeDeltas +
//               '}';
//    }
//
//    public static Builder builder()
//    {
//        return new Builder();
//    }
//
//    public static final class Builder
//    {
//        PlacementDeltas.Builder reads = PlacementDeltas.builder();
//        PlacementDeltas.Builder writes = PlacementDeltas.builder();
//
//        public Builder withReadDelta(ReplicationParams params, Delta delta)
//        {
//            reads.put(params, delta);
//            return this;
//        }
//
//        public Builder withWriteDelta(ReplicationParams params, Delta delta)
//        {
//            writes.put(params, delta);
//            return this;
//        }
//
//        public GlobalPlacementDelta build()
//        {
//            return new GlobalPlacementDelta(reads.build(), writes.build());
//        }
//    }
//
//    public static class Serializer implements IVersionedSerializer<GlobalPlacementDelta>
//    {
//
//        public void serialize(GlobalPlacementDelta t, DataOutputPlus out, int version) throws IOException
//        {
//            out.writeInt(t.readDeltas.size());
//            for (Map.Entry<ReplicationParams, Delta> e : t.readDeltas)
//            {
//                ReplicationParams.serializer.serialize(e.getKey(), out, version);
//                Delta.serializer.serialize(e.getValue(), out, version);
//            }
//            out.writeInt(t.writeDeltas.size());
//            for (Map.Entry<ReplicationParams, Delta> e : t.writeDeltas)
//            {
//                ReplicationParams.serializer.serialize(e.getKey(), out, version);
//                Delta.serializer.serialize(e.getValue(), out, version);
//            }
//        }
//
//        public GlobalPlacementDelta deserialize(DataInputPlus in, int version) throws IOException
//        {
//            int size = in.readInt();
//            PlacementDeltas.Builder reads = PlacementDeltas.builder(size);
//            for (int i = 0; i < size; i++)
//                reads.put(ReplicationParams.serializer.deserialize(in, version), Delta.serializer.deserialize(in, version));
//            size = in.readInt();
//            PlacementDeltas.Builder writes = PlacementDeltas.builder(size);
//            for (int i = 0; i < size; i++)
//                writes.put(ReplicationParams.serializer.deserialize(in, version), Delta.serializer.deserialize(in, version));
//
//            return new GlobalPlacementDelta(reads.build(), writes.build());
//        }
//
//        public long serializedSize(GlobalPlacementDelta t, int version)
//        {
//            long size = TypeSizes.INT_SIZE;
//            for (Map.Entry<ReplicationParams, Delta> e : t.readDeltas)
//            {
//                size += ReplicationParams.serializer.serializedSize(e.getKey(), version);
//                size += Delta.serializer.serializedSize(e.getValue(), version);
//            }
//            size += TypeSizes.INT_SIZE;
//            for (Map.Entry<ReplicationParams, Delta> e : t.writeDeltas)
//            {
//                size += ReplicationParams.serializer.serializedSize(e.getKey(), version);
//                size += Delta.serializer.serializedSize(e.getValue(), version);
//            }
//            return size;
//        }
//    }
//}
