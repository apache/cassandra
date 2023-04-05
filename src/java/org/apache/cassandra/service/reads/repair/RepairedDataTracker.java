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

package org.apache.cassandra.service.reads.repair;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.google.common.base.MoreObjects;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.ByteBufferUtil;

public class RepairedDataTracker
{
    private final RepairedDataVerifier verifier;

    public final Multimap<ByteBuffer, InetAddressAndPort> digests = HashMultimap.create();
    public final Set<InetAddressAndPort> inconclusiveDigests = new HashSet<>();

    public RepairedDataTracker(RepairedDataVerifier verifier)
    {
        this.verifier = verifier;
    }

    public void recordDigest(InetAddressAndPort source, ByteBuffer digest, boolean isConclusive)
    {
        digests.put(digest, source);
        if (!isConclusive)
            inconclusiveDigests.add(source);
    }

    public void verify()
    {
        verifier.verify(this);
    }

    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                          .add("digests", hexDigests())
                          .add("inconclusive", inconclusiveDigests).toString();
    }

    private Map<String, Collection<InetAddressAndPort>> hexDigests()
    {
        Map<String, Collection<InetAddressAndPort>> hexDigests = new HashMap<>();
        digests.asMap().forEach((k, v) -> hexDigests.put(ByteBufferUtil.bytesToHex(k), v));
        return hexDigests;
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RepairedDataTracker that = (RepairedDataTracker) o;
        return Objects.equals(digests, that.digests) &&
               Objects.equals(inconclusiveDigests, that.inconclusiveDigests);
    }

    public int hashCode()
    {
        return Objects.hash(digests, inconclusiveDigests);
    }
}
