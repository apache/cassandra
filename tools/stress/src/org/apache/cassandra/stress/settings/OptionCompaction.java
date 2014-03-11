package org.apache.cassandra.stress.settings;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.common.base.Function;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.exceptions.ConfigurationException;

/**
 * For specifying replication options
 */
class OptionCompaction extends OptionMulti
{

    private final OptionSimple strategy = new OptionSimple("strategy=", new StrategyAdapter(), null, "The compaction strategy to use", false);

    public OptionCompaction()
    {
        super("compaction", "Define the compaction strategy and any parameters", true);
    }

    public String getStrategy()
    {
        return strategy.value();
    }

    public Map<String, String> getOptions()
    {
        return extraOptions();
    }

    protected List<? extends Option> options()
    {
        return Arrays.asList(strategy);
    }

    @Override
    public boolean happy()
    {
        return true;
    }

    private static final class StrategyAdapter implements Function<String, String>
    {

        public String apply(String name)
        {
            try
            {
                CFMetaData.createCompactionStrategy(name);
            } catch (ConfigurationException e)
            {
                throw new IllegalArgumentException("Invalid compaction strategy: " + name);
            }
            return name;
        }
    }

}
