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

package org.apache.cassandra.repair.consistent.admin;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.management.openmbean.ArrayType;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;

import com.google.common.base.Preconditions;

import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.TimeUUID;

public class PendingStat
{
    private static final String[] COMPOSITE_NAMES = new String[] {"dataSize", "numSSTables", "sessions"};
    private static final OpenType<?>[] COMPOSITE_TYPES;
    public static final CompositeType COMPOSITE_TYPE;

    static
    {
        try
        {
            COMPOSITE_TYPES = new OpenType[] { SimpleType.LONG, SimpleType.INTEGER, ArrayType.getArrayType(SimpleType.STRING) };
            COMPOSITE_TYPE = new CompositeType(PendingStat.class.getName(),
                                               PendingStat.class.getSimpleName(),
                                               COMPOSITE_NAMES, COMPOSITE_NAMES, COMPOSITE_TYPES);
        }
        catch (OpenDataException e)
        {
            throw new RuntimeException(e);
        }
    }



    public final long dataSize;
    public final int numSSTables;
    public final Set<TimeUUID> sessions;

    public PendingStat(long dataSize, int numSSTables, Set<TimeUUID> sessions)
    {
        this.dataSize = dataSize;
        this.numSSTables = numSSTables;
        this.sessions = Collections.unmodifiableSet(sessions);
    }

    public String sizeString()
    {
        return String.format("%s (%s sstables / %s sessions)", FileUtils.stringifyFileSize(dataSize), numSSTables, sessions.size());
    }

    public CompositeData toComposite()
    {
        Map<String, Object> values = new HashMap<>();
        values.put(COMPOSITE_NAMES[0], dataSize);
        values.put(COMPOSITE_NAMES[1], numSSTables);
        String[] sessionIds = new String[sessions.size()];
        int idx = 0;
        for (TimeUUID session : sessions)
            sessionIds[idx++] = session.toString();
        values.put(COMPOSITE_NAMES[2], sessionIds);

        try
        {
            return new CompositeDataSupport(COMPOSITE_TYPE, values);
        }
        catch (OpenDataException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static PendingStat fromComposite(CompositeData cd)
    {
        Preconditions.checkArgument(cd.getCompositeType().equals(COMPOSITE_TYPE));
        Object[] values = cd.getAll(COMPOSITE_NAMES);
        Set<TimeUUID> sessions = new HashSet<>();
        for (String session : (String[]) values[2])
            sessions.add(TimeUUID.fromString(session));
        return new PendingStat((long) values[0], (int) values[1], sessions);
    }

    public static class Builder
    {
        public long dataSize = 0;
        public int numSSTables = 0;
        public Set<TimeUUID> sessions = new HashSet<>();

        public Builder addSSTable(SSTableReader sstable)
        {
            TimeUUID sessionID = sstable.getPendingRepair();
            if (sessionID == null)
                return this;
            dataSize += sstable.onDiskLength();
            sessions.add(sessionID);
            numSSTables++;
            return this;
        }

        public Builder addStat(PendingStat stat)
        {
            dataSize += stat.dataSize;
            numSSTables += stat.numSSTables;
            sessions.addAll(stat.sessions);
            return this;
        }

        public PendingStat build()
        {
            return new PendingStat(dataSize, numSSTables, sessions);
        }
    }
}
