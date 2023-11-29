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
import java.util.Map;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;

import com.google.common.base.Preconditions;

public class PendingStats
{

    private static final String[] COMPOSITE_NAMES = new String[] { "keyspace", "table", "total", "pending", "finalized", "failed" };
    private static final OpenType<?>[] COMPOSITE_TYPES;
    private static final CompositeType COMPOSITE_TYPE;

    static
    {
        try
        {
            COMPOSITE_TYPES = new OpenType[] { SimpleType.STRING,
                                               SimpleType.STRING,
                                               PendingStat.COMPOSITE_TYPE,
                                               PendingStat.COMPOSITE_TYPE,
                                               PendingStat.COMPOSITE_TYPE,
                                               PendingStat.COMPOSITE_TYPE};
            COMPOSITE_TYPE = new CompositeType(RepairStats.Section.class.getName(), "PendingStats", COMPOSITE_NAMES, COMPOSITE_NAMES, COMPOSITE_TYPES);
        }
        catch (OpenDataException e)
        {
            throw new RuntimeException(e);
        }
    }

    public final String keyspace;
    public final String table;
    public final PendingStat total;
    public final PendingStat pending;
    public final PendingStat finalized;
    public final PendingStat failed;

    public PendingStats(String keyspace, String table, PendingStat pending, PendingStat finalized, PendingStat failed)
    {
        this.keyspace = keyspace;
        this.table = table;

        this.total = new PendingStat.Builder().addStat(pending).addStat(finalized).addStat(failed).build();
        this.pending = pending;
        this.finalized = finalized;
        this.failed = failed;
    }

    public CompositeData toComposite()
    {
        Map<String, Object> values = new HashMap<>();
        values.put(COMPOSITE_NAMES[0], keyspace);
        values.put(COMPOSITE_NAMES[1], table);
        values.put(COMPOSITE_NAMES[2], total.toComposite());
        values.put(COMPOSITE_NAMES[3], pending.toComposite());
        values.put(COMPOSITE_NAMES[4], finalized.toComposite());
        values.put(COMPOSITE_NAMES[5], failed.toComposite());
        try
        {
            return new CompositeDataSupport(COMPOSITE_TYPE, values);
        }
        catch (OpenDataException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static PendingStats fromComposite(CompositeData cd)
    {
        Preconditions.checkArgument(cd.getCompositeType().equals(COMPOSITE_TYPE));
        Object[] values = cd.getAll(COMPOSITE_NAMES);
        return new PendingStats((String) values[0],
                                (String) values[1],
                                PendingStat.fromComposite((CompositeData) values[2]),
                                PendingStat.fromComposite((CompositeData) values[3]),
                                PendingStat.fromComposite((CompositeData) values[3]));

    }
}
