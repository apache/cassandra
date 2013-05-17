package org.apache.cassandra.triggers;

import java.io.File;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.cql.QueryProcessor;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.CounterMutation;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.FBUtilities;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class TriggerExecutor
{
    public static final TriggerExecutor instance = new TriggerExecutor();

    private final Map<String, ITrigger> cachedTriggers = Maps.newConcurrentMap();
    private final ClassLoader parent = Thread.currentThread().getContextClassLoader();
    private final File triggerDirectory = new File(FBUtilities.cassandraHomeDir(), "triggers");
    private volatile ClassLoader customClassLoader;

    private TriggerExecutor()
    {
        reloadClasses();
    }

    /**
     * Reload the triggers which is already loaded, Invoking this will update
     * the class loader so new jars can be loaded.
     */
    public void reloadClasses()
    {
        customClassLoader = new CustomClassLoader(parent, triggerDirectory);
        cachedTriggers.clear();
    }

    public Collection<RowMutation> execute(Collection<? extends IMutation> updates) throws InvalidRequestException
    {
        boolean hasCounters = false;
        Collection<RowMutation> tmutations = null;
        for (IMutation mutation : updates)
        {
            for (ColumnFamily cf : mutation.getColumnFamilies())
            {
                List<RowMutation> intermediate = execute(mutation.key(), cf);
                if (intermediate == null)
                    continue;

                validate(intermediate);
                if (tmutations == null)
                    tmutations = intermediate;
                else
                    tmutations.addAll(intermediate);
            }
            if (mutation instanceof CounterMutation)
                hasCounters = true;
        }
        if (tmutations != null && hasCounters)
            throw new InvalidRequestException("Counter mutations and trigger mutations cannot be applied together atomically.");
        return tmutations;
    }

    private void validate(Collection<RowMutation> tmutations) throws InvalidRequestException
    {
        for (RowMutation mutation : tmutations)
        {
            QueryProcessor.validateKey(mutation.key());
            for (ColumnFamily tcf : mutation.getColumnFamilies())
                for (ByteBuffer tName : tcf.getColumnNames())
                    QueryProcessor.validateColumn(tcf.metadata(), tName, tcf.getColumn(tName).value());
        }
    }

    /**
     * Switch class loader before using the triggers for the column family, if
     * not loaded them with the custom class loader.
     */
    private List<RowMutation> execute(ByteBuffer key, ColumnFamily columnFamily)
    {
        Set<String> triggerNames = columnFamily.metadata().getTriggerClass();
        if (triggerNames == null)
            return null;
        List<RowMutation> tmutations = Lists.newLinkedList();
        Thread.currentThread().setContextClassLoader(customClassLoader);
        try
        {
            for (String triggerName : triggerNames)
            {
                ITrigger trigger = cachedTriggers.get(triggerName);
                if (trigger == null)
                {
                    trigger = loadTriggerInstance(triggerName);
                    cachedTriggers.put(triggerName, trigger);
                }
                Collection<RowMutation> temp = trigger.augment(key, columnFamily);
                if (temp != null)
                    tmutations.addAll(temp);
            }
            return tmutations;
        }
        catch (Exception ex)
        {
            throw new RuntimeException(String.format("Exception while creating trigger a trigger on CF with ID: %s", columnFamily.id()), ex);
        }
        finally
        {
            Thread.currentThread().setContextClassLoader(parent);
        }
    }

    private synchronized ITrigger loadTriggerInstance(String triggerName) throws Exception
    {
        // double check.
        if (cachedTriggers.get(triggerName) != null)
            return cachedTriggers.get(triggerName);

        Constructor<? extends ITrigger> costructor = (Constructor<? extends ITrigger>) customClassLoader.loadClass(triggerName).getConstructor(new Class<?>[0]);
        return costructor.newInstance(new Object[0]);
    }
}
