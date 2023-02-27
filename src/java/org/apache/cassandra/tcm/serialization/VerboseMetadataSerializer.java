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

package org.apache.cassandra.tcm.serialization;

import java.io.IOException;

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.vint.VIntCoding;

public class VerboseMetadataSerializer
{

    // These first two methods, which don't accept a version are just for inital
    // convenience and should be removed when we have multiple supported metadata
    // versions. At that point, callers of this should discover the lowest supported
    // metadata version across the cluster and supply that directly.

    // We don't want to necessarily do that lookup in the methods themselves as these
    // two are often used together, and when this is the case, they should always be
    // called with the same version.
    // e.g.:
    // 1) call to serializedSize with V0 returns X
    // 2) allocate a buffer B of size X
    // 3) lowest supported version changes to V1, which requires X + 1 bytes
    // 4) call to serialize, with V1 and buffer B
    public static <In, Out> void serialize(AsymmetricMetadataSerializer<In, Out> base,
                                           In t,
                                           DataOutputPlus out) throws IOException
    {
        serialize(base, t, out, Version.V0);
    }

    public static <In, Out> long serializedSize(AsymmetricMetadataSerializer<In, Out> base, In t)
    {
        return serializedSize(base, t, Version.V0);
    }

    public static <In, Out> void serialize(AsymmetricMetadataSerializer<In, Out> base,
                                           In t,
                                           DataOutputPlus out,
                                           Version version) throws IOException
    {
        out.writeUnsignedVInt32(version.asInt());
        base.serialize(t, out, version);
    }

    public static <In, Out> Out deserialize(AsymmetricMetadataSerializer<In, Out> base,
                                            DataInputPlus in) throws IOException
    {
        int x = in.readUnsignedVInt32();
        Version v = Version.fromInt(x);
        return base.deserialize(in, v);
    }

    public static <In, Out> long serializedSize(AsymmetricMetadataSerializer<In, Out> base,
                                                In t,
                                                Version version)
    {
        return VIntCoding.computeUnsignedVIntSize(version.asInt()) +
               base.serializedSize(t, version);
    }
}
