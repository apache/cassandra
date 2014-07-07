package org.apache.cassandra.stress.settings;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.math3.util.Pair;

public final class OptionAnyProbabilities extends OptionMulti
{
    public OptionAnyProbabilities(String name, String description)
    {
        super(name, description, false);
    }

    final CollectRatios ratios = new CollectRatios();

    private static final class CollectRatios extends Option
    {
        Map<String, Double> options = new LinkedHashMap<>();

        boolean accept(String param)
        {
            String[] args = param.split("=");
            if (args.length == 2 && args[1].length() > 0 && args[0].length() > 0)
            {
                if (options.put(args[0], Double.parseDouble(args[1])) != null)
                    throw new IllegalArgumentException(args[0] + " set twice");
                return true;
            }
            return false;
        }

        boolean happy()
        {
            return !options.isEmpty();
        }

        String shortDisplay()
        {
            return null;
        }

        String longDisplay()
        {
            return null;
        }

        List<String> multiLineDisplay()
        {
            return Collections.emptyList();
        }

        boolean setByUser()
        {
            return !options.isEmpty();
        }
    }


    @Override
    public List<? extends Option> options()
    {
        return Arrays.asList(ratios);
    }

    List<Pair<String, Double>> ratios()
    {
        List<Pair<String, Double>> ratiosOut = new ArrayList<>();
        for (Map.Entry<String, Double> e : ratios.options.entrySet())
            ratiosOut.add(new Pair<String, Double>(e.getKey(), e.getValue()));
        return ratiosOut;
    }
}

