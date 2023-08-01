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
import com.google.common.collect.Sets;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.utils.TimeUUID;

public class CleanupSummary
{
    private static final String[] COMPOSITE_NAMES = new String[] { "keyspace", "table", "successful", "unsuccessful" };
    private static final OpenType<?>[] COMPOSITE_TYPES;
    private static final CompositeType COMPOSITE_TYPE;

    static
    {
        try
        {
            COMPOSITE_TYPES = new OpenType[] { SimpleType.STRING,
                                               SimpleType.STRING,
                                               ArrayType.getArrayType(SimpleType.STRING),
                                               ArrayType.getArrayType(SimpleType.STRING) };
            COMPOSITE_TYPE = new CompositeType(RepairStats.Section.class.getName(), "PendingStats",
                                               COMPOSITE_NAMES, COMPOSITE_NAMES, COMPOSITE_TYPES);
        }
        catch (OpenDataException e)
        {
            throw new RuntimeException(e);
        }
    }

    public final String keyspace;
    public final String table;

    public final Set<TimeUUID> successful;
    public final Set<TimeUUID> unsuccessful;

    public CleanupSummary(String keyspace, String table, Set<TimeUUID> successful, Set<TimeUUID> unsuccessful)
    {
        this.keyspace = keyspace;
        this.table = table;
        this.successful = successful;
        this.unsuccessful = unsuccessful;
    }

    public CleanupSummary(ColumnFamilyStore cfs, Set<TimeUUID> successful, Set<TimeUUID> unsuccessful)
    {
        this(cfs.getKeyspaceName(), cfs.name, successful, unsuccessful);
    }

    public static CleanupSummary add(CleanupSummary l, CleanupSummary r)
    {
        Preconditions.checkArgument(l.keyspace.equals(r.keyspace));
        Preconditions.checkArgument(l.table.equals(r.table));

        Set<TimeUUID> unsuccessful = new HashSet<>(l.unsuccessful);
        unsuccessful.addAll(r.unsuccessful);

        Set<TimeUUID> successful = new HashSet<>(l.successful);
        successful.addAll(r.successful);
        successful.removeAll(unsuccessful);

        return new CleanupSummary(l.keyspace, l.table, successful, unsuccessful);
    }

    private static String[] uuids2Strings(Set<TimeUUID> uuids)
    {
        String[] strings = new String[uuids.size()];
        int idx = 0;
        for (TimeUUID uuid : uuids)
            strings[idx++] = uuid.toString();
        return strings;
    }

    private static Set<TimeUUID> strings2Uuids(String[] strings)
    {
        Set<TimeUUID> uuids = Sets.newHashSetWithExpectedSize(strings.length);
        for (String string : strings)
            uuids.add(TimeUUID.fromString(string));

        return uuids;
    }

    public CompositeData toComposite()
    {
        Map<String, Object> values = new HashMap<>();
        values.put(COMPOSITE_NAMES[0], keyspace);
        values.put(COMPOSITE_NAMES[1], table);
        values.put(COMPOSITE_NAMES[2], uuids2Strings(successful));
        values.put(COMPOSITE_NAMES[3], uuids2Strings(unsuccessful));
        try
        {
            return new CompositeDataSupport(COMPOSITE_TYPE, values);
        }
        catch (OpenDataException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static CleanupSummary fromComposite(CompositeData cd)
    {
        Preconditions.checkArgument(cd.getCompositeType().equals(COMPOSITE_TYPE));
        Object[] values = cd.getAll(COMPOSITE_NAMES);
        return new CleanupSummary((String) values[0],
                                  (String) values[1],
                                  strings2Uuids((String[]) values[2]),
                                  strings2Uuids((String[]) values[3]));

    }
}
