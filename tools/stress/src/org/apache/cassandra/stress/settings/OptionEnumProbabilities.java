package org.apache.cassandra.stress.settings;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.math3.util.Pair;

public final class OptionEnumProbabilities<T> extends OptionMulti
{
    final List<OptMatcher<T>> options;

    public static class Opt<T>
    {
        final T option;
        final String defaultValue;

        public Opt(T option, String defaultValue)
        {
            this.option = option;
            this.defaultValue = defaultValue;
        }
    }

    private static final class OptMatcher<T> extends OptionSimple
    {
        final T opt;
        OptMatcher(T opt, String defaultValue)
        {
            super(opt.toString().toLowerCase() + "=", "[0-9]+(\\.[0-9]+)?", defaultValue, "Performs this many " + opt + " operations out of total", false);
            this.opt = opt;
        }
    }

    public OptionEnumProbabilities(List<Opt<T>> universe, String name, String description)
    {
        super(name, description, false);
        List<OptMatcher<T>> options = new ArrayList<>();
        for (Opt<T> option : universe)
            options.add(new OptMatcher<T>(option.option, option.defaultValue));
        this.options = options;
    }

    @Override
    public List<? extends Option> options()
    {
        return options;
    }

    List<Pair<T, Double>> ratios()
    {
        List<? extends Option> ratiosIn = setByUser() ? optionsSetByUser() : defaultOptions();
        List<Pair<T, Double>> ratiosOut = new ArrayList<>();
        for (Option opt : ratiosIn)
        {
            OptMatcher<T> optMatcher = (OptMatcher<T>) opt;
            double d = Double.parseDouble(optMatcher.value());
            ratiosOut.add(new Pair<>(optMatcher.opt, d));
        }
        return ratiosOut;
    }
}

