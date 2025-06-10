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

package org.apache.cassandra.tcm;

import java.io.IOException;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;

import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.utils.btree.BTreeSet;

public class CMSMembership implements MetadataValue<CMSMembership>
{
    public static final Serializer serializer = new Serializer();
    public static final CMSMembership EMPTY = new CMSMembership();

    private final Epoch lastModified;
    private final BTreeSet<NodeId> fullMembers;
    private final BTreeSet<NodeId> joiningMembers;

    private CMSMembership()
    {
        this(Epoch.EMPTY,
             BTreeSet.empty(NodeId::compareTo),
             BTreeSet.empty(NodeId::compareTo));
    }

    private CMSMembership(Epoch lastModified, BTreeSet<NodeId> fullMembers, BTreeSet<NodeId> joiningMembers)
    {
        this.lastModified = lastModified;
        this.fullMembers = fullMembers;
        this.joiningMembers = joiningMembers;
    }


    @Override
    public CMSMembership withLastModified(Epoch epoch)
    {
        return lastModified.is(epoch) ? this : new CMSMembership(epoch, fullMembers, joiningMembers);
    }

    @Override
    public Epoch lastModified()
    {
        return lastModified;
    }

    public ImmutableSet<NodeId> joiningMembers()
    {
        return ImmutableSet.copyOf(joiningMembers);
    }

    public ImmutableSet<NodeId> fullMembers()
    {
        return ImmutableSet.copyOf(fullMembers);
    }

    public CMSMembership startJoining(NodeId id)
    {
        if (joiningMembers.contains(id))
            throw new IllegalStateException(id + " is already joining the CMS");
        if (fullMembers.contains(id))
            throw new IllegalStateException(id + " has already fully joined the CMS");

        return new CMSMembership(lastModified, fullMembers, joiningMembers.with(id));
    }

    public CMSMembership cancelJoining(NodeId id)
    {
        if (!joiningMembers.contains(id))
            throw new IllegalStateException(id + " is not currently joining the CMS");
        if (fullMembers.contains(id))
            throw new IllegalStateException(id + " has already fully joined the CMS");

        return new CMSMembership(lastModified, fullMembers, joiningMembers.without(id));
    }

    public CMSMembership finishJoining(NodeId id)
    {
        if (!joiningMembers.contains(id))
            throw new IllegalStateException(id + " is not currently joining the CMS");
        if (fullMembers.contains(id))
            throw new IllegalStateException(id + " has already fully joined the CMS");

        return new CMSMembership(lastModified, fullMembers.with(id), joiningMembers.without(id));
    }

    public CMSMembership leave(NodeId id)
    {
        if (joiningMembers.contains(id))
            throw new IllegalStateException(id + " is currently joining the CMS, ");
        if (!fullMembers.contains(id))
            throw new IllegalStateException(id + " is not a CMS member");

        return new CMSMembership(lastModified, fullMembers.without(id), joiningMembers);
    }

    @Override
    public String toString()
    {
        return "CMSMembership{" +
               "lastModified=" + lastModified +
               ", fullMembers=" + fullMembers +
               ", joiningMembers=" + joiningMembers +
               '}';
    }

    @Override
    public final boolean equals(Object o)
    {
        if (!(o instanceof CMSMembership)) return false;

        CMSMembership that = (CMSMembership) o;
        return Objects.equals(lastModified, that.lastModified) &&
               Objects.equals(fullMembers, that.fullMembers) &&
               Objects.equals(joiningMembers, that.joiningMembers);
    }

    @Override
    public int hashCode()
    {
        int result = Objects.hashCode(lastModified);
        result = 31 * result + Objects.hashCode(fullMembers);
        result = 31 * result + Objects.hashCode(joiningMembers);
        return result;
    }

    public static class Serializer implements MetadataSerializer<CMSMembership>
    {
        @Override
        public void serialize(CMSMembership t, DataOutputPlus out, Version version) throws IOException
        {
            Epoch.serializer.serialize(t.lastModified, out);

            out.writeUnsignedVInt32(t.fullMembers.size());
            for (NodeId id : t.fullMembers)
                NodeId.serializer.serialize(id, out, version);

            out.writeUnsignedVInt32(t.joiningMembers.size());
            for (NodeId id : t.joiningMembers)
                NodeId.serializer.serialize(id, out, version);
        }

        @Override
        public CMSMembership deserialize(DataInputPlus in, Version version) throws IOException
        {
            Epoch lastModified = Epoch.serializer.deserialize(in, version);

            int fullMemberCount = in.readUnsignedVInt32();
            SortedSet<NodeId> fullMembers = new TreeSet<>();
            for (int i = 0; i < fullMemberCount; i++)
                fullMembers.add(NodeId.serializer.deserialize(in, version));

            int joiningMemberCount = in.readUnsignedVInt32();
            SortedSet<NodeId> joiningMembers = new TreeSet<>();
            for (int i = 0; i < joiningMemberCount; i++)
                joiningMembers.add(NodeId.serializer.deserialize(in, version));

            return new CMSMembership(lastModified, BTreeSet.of(fullMembers), BTreeSet.of(joiningMembers)) ;
        }

        @Override
        public long serializedSize(CMSMembership t, Version version)
        {
            long size = Epoch.serializer.serializedSize(t.lastModified);

            size += TypeSizes.sizeofUnsignedVInt(t.fullMembers.size());
            for (NodeId id : t.fullMembers)
                size += NodeId.serializer.serializedSize(id, version);

            size += TypeSizes.sizeofUnsignedVInt(t.joiningMembers.size());
            for (NodeId id : t.joiningMembers)
                size += NodeId.serializer.serializedSize(id, version);

            return size;
        }
    }

}
