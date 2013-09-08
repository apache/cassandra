/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.config;

import java.util.*;

import com.google.common.base.Objects;

import org.apache.cassandra.cql3.ColumnNameBuilder;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.thrift.TriggerDef;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

public class TriggerDefinition
{
    private static final String TRIGGER_NAME = "trigger_name";
    private static final String TRIGGER_OPTIONS = "trigger_options";
    private static final String CLASS = "class";

    public final String name;

    // For now, the only supported option is 'class'.
    // Proper trigger parametrization will be added later.
    public final String classOption;

    TriggerDefinition(String name, String classOption)
    {
        this.name = name;
        this.classOption = classOption;
    }

    public static TriggerDefinition create(String name, String classOption)
    {
        return new TriggerDefinition(name, classOption);
    }

    /**
     * Deserialize triggers from storage-level representation.
     *
     * @param serializedTriggers storage-level partition containing the trigger definitions
     * @return the list of processed TriggerDefinitions
     */
    public static List<TriggerDefinition> fromSchema(Row serializedTriggers)
    {
        List<TriggerDefinition> triggers = new ArrayList<>();
        String query = String.format("SELECT * FROM %s.%s", Keyspace.SYSTEM_KS, SystemKeyspace.SCHEMA_TRIGGERS_CF);
        for (UntypedResultSet.Row row : QueryProcessor.resultify(query, serializedTriggers))
        {
            String name = row.getString(TRIGGER_NAME);
            String classOption = row.getMap(TRIGGER_OPTIONS, UTF8Type.instance, UTF8Type.instance).get(CLASS);
            triggers.add(new TriggerDefinition(name, classOption));
        }
        return triggers;
    }

    /**
     * Add specified trigger to the schema using given row.
     *
     * @param rm        The schema row mutation
     * @param cfName    The name of the parent ColumnFamily
     * @param timestamp The timestamp to use for the columns
     */
    public void toSchema(RowMutation rm, String cfName, long timestamp)
    {
        ColumnFamily cf = rm.addOrGet(SystemKeyspace.SCHEMA_TRIGGERS_CF);

        ColumnNameBuilder builder = CFMetaData.SchemaTriggersCf.getCfDef().getColumnNameBuilder();
        builder.add(bytes(cfName)).add(bytes(name));

        cf.addColumn(builder.copy().add(bytes("")).build(), bytes(""), timestamp); // the row marker
        cf.addColumn(builder.copy().add(bytes(TRIGGER_OPTIONS)).add(bytes(CLASS)).build(), bytes(classOption), timestamp);
    }

    /**
     * Drop specified trigger from the schema using given row.
     *
     * @param rm        The schema row mutation
     * @param cfName    The name of the parent ColumnFamily
     * @param timestamp The timestamp to use for the tombstone
     */
    public void deleteFromSchema(RowMutation rm, String cfName, long timestamp)
    {
        ColumnFamily cf = rm.addOrGet(SystemKeyspace.SCHEMA_TRIGGERS_CF);
        int ldt = (int) (System.currentTimeMillis() / 1000);

        ColumnNameBuilder builder = CFMetaData.SchemaTriggersCf.getCfDef().getColumnNameBuilder();
        builder.add(bytes(cfName)).add(bytes(name));
        cf.addAtom(new RangeTombstone(builder.build(), builder.buildAsEndOfRange(), timestamp, ldt));
    }

    public static TriggerDefinition fromThrift(TriggerDef thriftDef)
    {
        return new TriggerDefinition(thriftDef.getName(), thriftDef.getOptions().get(CLASS));
    }

    public TriggerDef toThrift()
    {
        TriggerDef td = new TriggerDef();
        td.setName(name);
        td.setOptions(Collections.singletonMap(CLASS, classOption));
        return td;
    }

    public static Map<String, TriggerDefinition> fromThrift(List<TriggerDef> thriftDefs)
    {
        Map<String, TriggerDefinition> triggerDefinitions = new HashMap<>();
        for (TriggerDef thriftDef : thriftDefs)
            triggerDefinitions.put(thriftDef.getName(), fromThrift(thriftDef));
        return triggerDefinitions;
    }

    public static List<TriggerDef> toThrift(Map<String, TriggerDefinition> triggers)
    {
        List<TriggerDef> thriftDefs = new ArrayList<>(triggers.size());
        for (TriggerDefinition def : triggers.values())
            thriftDefs.add(def.toThrift());
        return thriftDefs;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof TriggerDefinition))
            return false;

        TriggerDefinition td = (TriggerDefinition) o;

        return Objects.equal(name, td.name) && Objects.equal(classOption, td.classOption);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(name, classOption);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this).add("name", name).add("classOption", classOption).toString();
    }
}
