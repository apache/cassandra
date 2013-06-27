package org.apache.cassandra.config;

import static org.apache.cassandra.cql3.QueryProcessor.processInternal;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.cassandra.cql3.ColumnNameBuilder;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.UntypedResultSet.Row;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.utils.ByteBufferUtil;

public class TriggerOptions
{
    private static final String CLASS_KEY = "class";
    private static final String OPTIONS_KEY = "trigger_options";

    public static Map<String, Map<String, String>> getAllTriggers(String ksName, String cfName)
    {
        String req = "SELECT * FROM system.%s WHERE keyspace_name='%s' AND column_family='%s'";
        UntypedResultSet result = processInternal(String.format(req, SystemKeyspace.SCHEMA_TRIGGERS_CF, ksName, cfName));
        Map<String, Map<String, String>> triggers = new HashMap<>();
        if (result.isEmpty())
            return triggers;
        for (Row row : result)
            triggers.put(row.getString("trigger_name"), row.getMap(OPTIONS_KEY, UTF8Type.instance, UTF8Type.instance));
        return triggers;
    }

    public static void addColumns(RowMutation rm, String cfName, Entry<String, Map<String, String>> tentry, long modificationTimestamp)
    {
        ColumnFamily cf = rm.addOrGet(SystemKeyspace.SCHEMA_TRIGGERS_CF);
        assert tentry.getValue().get(CLASS_KEY) != null;
        ColumnNameBuilder builder = CFMetaData.SchemaTriggerCf.getCfDef().getColumnNameBuilder();
        builder.add(ByteBufferUtil.bytes(cfName)).add(ByteBufferUtil.bytes(tentry.getKey())).add(ByteBufferUtil.bytes(OPTIONS_KEY));
        for (Entry<String, String> entry : tentry.getValue().entrySet())
        {
            ColumnNameBuilder builderCopy = builder.copy();
            builderCopy.add(ByteBufferUtil.bytes(entry.getKey()));
            cf.addColumn(builderCopy.build(), ByteBufferUtil.bytes(entry.getValue()), modificationTimestamp);
        }
    }

    public static void deleteColumns(RowMutation rm, String cfName, Entry<String, Map<String, String>> tentry, long modificationTimestamp)
    {
        ColumnFamily cf = rm.addOrGet(SystemKeyspace.SCHEMA_TRIGGERS_CF);
        int ldt = (int) (System.currentTimeMillis() / 1000);
        ColumnNameBuilder builder = CFMetaData.SchemaTriggerCf.getCfDef().getColumnNameBuilder();
        builder.add(ByteBufferUtil.bytes(cfName)).add(ByteBufferUtil.bytes(tentry.getKey()));
        cf.addAtom(new RangeTombstone(builder.build(), builder.buildAsEndOfRange(), modificationTimestamp, ldt));
    }

    public static void update(CFMetaData cfm, String triggerName, String clazz)
    {
        Map<String, Map<String, String>> existingTriggers = cfm.getTriggers();
        assert existingTriggers.get(triggerName) == null;
        Map<String, String> triggerUnit = new HashMap<>();
        triggerUnit.put(CLASS_KEY, clazz);
        existingTriggers.put(triggerName, triggerUnit);
        cfm.triggers(existingTriggers);
    }

    public static void remove(CFMetaData cfm, String triggerName)
    {
        Map<String, Map<String, String>> existingTriggers = cfm.getTriggers(); // have a copy of the triggers
        existingTriggers.remove(triggerName);
        cfm.triggers(existingTriggers);
    }

    public static boolean hasTrigger(CFMetaData cfm, String triggerName)
    {
        return cfm.getTriggers().get(triggerName) != null;
    }

    public static Collection<String> extractClasses(Map<String, Map<String, String>> triggers)
    {
        List<String> classes = new ArrayList<>();
        if (triggers.isEmpty())
            return null;
        for (Map<String, String> options : triggers.values())
            classes.add(options.get(CLASS_KEY));
        return classes;
    }
}
