package org.apache.cassandra.stress.settings;

import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.stress.generatedata.Distribution;
import org.apache.cassandra.stress.generatedata.DistributionBoundApache;
import org.apache.cassandra.stress.generatedata.DistributionFactory;
import org.apache.cassandra.stress.generatedata.DistributionFixed;
import org.apache.commons.math3.distribution.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * For specifying replication options
 */
class OptionReplication extends Option
{

    private static final Pattern FULL = Pattern.compile("replication\\((.*)\\)", Pattern.CASE_INSENSITIVE);
    private static final Pattern OPTION = Pattern.compile("([^,=]+)=([^,]+)", Pattern.CASE_INSENSITIVE);

    private String strategy = "org.apache.cassandra.locator.SimpleStrategy";
    private Map<String, String> options = new HashMap<>();

    public String getStrategy()
    {
        return strategy;
    }

    public Map<String, String> getOptions()
    {
        if (!options.containsKey("replication_factor") && strategy.endsWith("SimpleStrategy"))
            options.put("replication_factor", "1");
        return options;
    }


    @Override
    public boolean accept(String param)
    {
        Matcher m = FULL.matcher(param);
        if (!m.matches())
            return false;
        String args = m.group(1);
        m = OPTION.matcher(args);
        int last = -1;
        while (m.find())
        {
            if (m.start() != last + 1)
                throw new IllegalArgumentException("Invalid replication specification: " + param);
            last = m.end();
            String key = m.group(1).toLowerCase();
            sw: switch(key)
            {
                case "factor":
                    try
                    {
                        Integer.parseInt(m.group(2));
                    } catch (NumberFormatException e)
                    {
                        throw new IllegalArgumentException("Invalid replication factor: " + param);
                    }
                    options.put("replication_factor", m.group(2));
                    break;
                case "strategy":
                    for (String name : new String[] { m.group(2), "org.apache.cassandra.locator." + m.group(2) })
                    {
                        try
                        {
                            Class<?> clazz = Class.forName(name);
                            if (!AbstractReplicationStrategy.class.isAssignableFrom(clazz))
                                throw new RuntimeException();
                            strategy = name;
                            break sw;
                        } catch (Exception _)
                        {
                        }
                    }
                    throw new IllegalArgumentException("Invalid replication strategy: " + param);
                default:

            }
        }
        return true;
    }

    @Override
    public boolean happy()
    {
        return true;
    }

    @Override
    public String shortDisplay()
    {
        return "replication(?)";
    }

    @Override
    public String longDisplay()
    {
        return "replication(factor=?,strategy=?,<option1>=?,...)";
    }

    @Override
    public List<String> multiLineDisplay()
    {
        return Arrays.asList(
                GroupedOptions.formatMultiLine("factor=?","The replication factor to use (default 1)"),
                GroupedOptions.formatMultiLine("strategy=?","The replication strategy to use (default SimpleStrategy)"),
                GroupedOptions.formatMultiLine("option=?","Arbitrary replication strategy options")
        );
    }

}
