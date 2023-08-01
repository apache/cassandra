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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.management.openmbean.*;

import com.google.common.base.Preconditions;

import org.apache.cassandra.repair.consistent.RepairedState;

public class RepairStats
{
    public static class Section
    {

        private static final String[] COMPOSITE_NAMES = new String[] { "start", "end", "repairedAt" };
        private static final OpenType<?>[] COMPOSITE_TYPES;
        private static final CompositeType COMPOSITE_TYPE;

        static
        {
            try
            {
                COMPOSITE_TYPES = new OpenType[] { SimpleType.STRING, SimpleType.STRING, SimpleType.LONG };
                COMPOSITE_TYPE = new CompositeType(Section.class.getName(), "Section", COMPOSITE_NAMES, COMPOSITE_NAMES, COMPOSITE_TYPES);
            }
            catch (OpenDataException e)
            {
                throw new RuntimeException(e);
            }
        }

        public final String start;
        public final String end;
        public final long time;

        public Section(String start, String end, long time)
        {
            this.start = start;
            this.end = end;
            this.time = time;
        }

        private CompositeData toComposite()
        {
            Map<String, Object> values = new HashMap<>();
            values.put(COMPOSITE_NAMES[0], start);
            values.put(COMPOSITE_NAMES[1], end);
            values.put(COMPOSITE_NAMES[2], time);

            try
            {
                return new CompositeDataSupport(COMPOSITE_TYPE, values);
            }
            catch (OpenDataException e)
            {
                throw new RuntimeException(e);
            }
        }

        private static Section fromComposite(CompositeData cd)
        {
            Preconditions.checkArgument(cd.getCompositeType().equals(COMPOSITE_TYPE));
            Object[] values = cd.getAll(COMPOSITE_NAMES);
            String start = (String) values[0];
            String end = (String) values[1];
            long time = (long) values[2];
            return new Section(start, end, time);
        }

        @Override
        public String toString()
        {
            return String.format("(%s,%s]=%s", start, end, time);
        }
    }

    private static final String[] COMPOSITE_NAMES = new String[] { "keyspace", "table", "minRepaired", "maxRepaired", "sections" };
    private static final OpenType<?>[] COMPOSITE_TYPES;
    private static final CompositeType COMPOSITE_TYPE;

    static
    {
        try
        {
            COMPOSITE_TYPES = new OpenType[] { SimpleType.STRING, SimpleType.STRING,
                                               SimpleType.LONG, SimpleType.LONG,
                                               ArrayType.getArrayType(Section.COMPOSITE_TYPE)};
            COMPOSITE_TYPE = new CompositeType(RepairStats.class.getName(), RepairStats.class.getSimpleName(),
                                               COMPOSITE_NAMES, COMPOSITE_NAMES, COMPOSITE_TYPES);
        }
        catch (OpenDataException e)
        {
            throw new RuntimeException(e);
        }
    }

    public final String keyspace;
    public final String table;
    public final long minRepaired;
    public final long maxRepaired;
    public final List<Section> sections;

    private RepairStats(String keyspace, String table, long minRepaired, long maxRepaired, List<Section> sections)
    {
        this.keyspace = keyspace;
        this.table = table;
        this.minRepaired = minRepaired;
        this.maxRepaired = maxRepaired;
        this.sections = sections;
    }

    public static List<Section> convertSections(List<RepairedState.Section> from)
    {
        List<Section> to = new ArrayList<>(from.size());
        for (RepairedState.Section section : from)
        {
            to.add(new Section(section.range.left.toString(), section.range.right.toString(), section.repairedAt));
        }
        return to;
    }

    public static RepairStats fromRepairState(String keyspace, String table, RepairedState.Stats stats)
    {
        return new RepairStats(keyspace, table, stats.minRepaired, stats.maxRepaired, convertSections(stats.sections));
    }

    public CompositeData toComposite()
    {
        Map<String, Object> values = new HashMap<>();
        values.put(COMPOSITE_NAMES[0], keyspace);
        values.put(COMPOSITE_NAMES[1], table);
        values.put(COMPOSITE_NAMES[2], minRepaired);
        values.put(COMPOSITE_NAMES[3], maxRepaired);

        CompositeData[] compositeSections = new CompositeData[sections.size()];
        for (int i=0; i<sections.size(); i++)
            compositeSections[i] = sections.get(i).toComposite();

        values.put(COMPOSITE_NAMES[4], compositeSections);
        try
        {
            return new CompositeDataSupport(COMPOSITE_TYPE, values);
        }
        catch (OpenDataException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static RepairStats fromComposite(CompositeData cd)
    {
        Preconditions.checkArgument(cd.getCompositeType().equals(COMPOSITE_TYPE));
        Object[] values = cd.getAll(COMPOSITE_NAMES);

        String keyspace = (String) values[0];
        String table = (String) values[1];
        long minRepaired = (long) values[2];
        long maxRepaired = (long) values[3];
        CompositeData[] sectionData = (CompositeData[]) values[4];
        List<Section> sections = new ArrayList<>(sectionData.length);
        for (CompositeData scd : sectionData)
            sections.add(Section.fromComposite(scd));
        return new RepairStats(keyspace, table, minRepaired, maxRepaired, sections);
    }
}
