package org.apache.cassandra.stress.settings;

import org.apache.cassandra.thrift.ConsistencyLevel;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

// Generic command settings - common to read/write/etc
public class SettingsCommand implements Serializable
{

    public final Command type;
    public final long count;
    public final int tries;
    public final boolean ignoreErrors;
    public final ConsistencyLevel consistencyLevel;
    public final double targetUncertainty;
    public final int minimumUncertaintyMeasurements;
    public final int maximumUncertaintyMeasurements;

    public SettingsCommand(Command type, GroupedOptions options)
    {
        this(type, (Options) options,
                options instanceof Count ? (Count) options : null,
                options instanceof Uncertainty ? (Uncertainty) options : null
        );
    }

    public SettingsCommand(Command type, Options options, Count count, Uncertainty uncertainty)
    {
        this.type = type;
        this.tries = Math.max(1, Integer.parseInt(options.retries.value()) + 1);
        this.ignoreErrors = options.ignoreErrors.setByUser();
        this.consistencyLevel = ConsistencyLevel.valueOf(options.consistencyLevel.value().toUpperCase());
        if (count != null)
        {
            this.count = Long.parseLong(count.count.value());
            this.targetUncertainty = -1;
            this.minimumUncertaintyMeasurements = -1;
            this.maximumUncertaintyMeasurements = -1;
        }
        else
        {
            this.count = -1;
            this.targetUncertainty = Double.parseDouble(uncertainty.uncertainty.value());
            this.minimumUncertaintyMeasurements = Integer.parseInt(uncertainty.minMeasurements.value());
            this.maximumUncertaintyMeasurements = Integer.parseInt(uncertainty.maxMeasurements.value());
        }
    }

    // Option Declarations

    static abstract class Options extends GroupedOptions
    {
        final OptionSimple retries = new OptionSimple("tries=", "[0-9]+", "9", "Number of tries to perform for each operation before failing", false);
        final OptionSimple ignoreErrors = new OptionSimple("ignore_errors", "", null, "Do not print/log errors", false);
        final OptionSimple consistencyLevel = new OptionSimple("cl=", "ONE|QUORUM|LOCAL_QUORUM|EACH_QUORUM|ALL|ANY", "ONE", "Consistency level to use", false);
    }

    static class Count extends Options
    {

        final OptionSimple count = new OptionSimple("n=", "[0-9]+", null, "Number of operations to perform", true);

        @Override
        public List<? extends Option> options()
        {
            return Arrays.asList(count, retries, ignoreErrors, consistencyLevel);
        }
    }

    static class Uncertainty extends Options
    {

        final OptionSimple uncertainty = new OptionSimple("err<", "0\\.[0-9]+", "0.02", "Run until the standard error of the mean is below this fraction", false);
        final OptionSimple minMeasurements = new OptionSimple("n>", "[0-9]+", "30", "Run at least this many iterations before accepting uncertainty convergence", false);
        final OptionSimple maxMeasurements = new OptionSimple("n<", "[0-9]+", "200", "Run at most this many iterations before accepting uncertainty convergence", false);

        @Override
        public List<? extends Option> options()
        {
            return Arrays.asList(uncertainty, minMeasurements, maxMeasurements, retries, ignoreErrors, consistencyLevel);
        }
    }

    // CLI Utility Methods

    static SettingsCommand get(Map<String, String[]> clArgs)
    {
        for (Command cmd : Command.values())
        {
            if (cmd.category == null)
                continue;
            final String[] params = clArgs.remove(cmd.toString().toLowerCase());
            if (params != null)
            {
                switch (cmd.category)
                {
                    case BASIC:
                        return build(cmd, params);
                    case MULTI:
                        return SettingsCommandMulti.build(cmd, params);
                    case MIXED:
                        return SettingsCommandMixed.build(params);
                }
            }
        }
        return null;
    }

    static SettingsCommand build(Command type, String[] params)
    {
        GroupedOptions options = GroupedOptions.select(params, new Count(), new Uncertainty());
        if (options == null)
        {
            printHelp(type);
            System.out.println("Invalid " + type + " options provided, see output for valid options");
            System.exit(1);
        }
        return new SettingsCommand(type, options);
    }

    static void printHelp(Command type)
    {
        printHelp(type.toString().toLowerCase());
    }

    static void printHelp(String type)
    {
        GroupedOptions.printOptions(System.out, type.toString().toLowerCase(), new Uncertainty(), new Count());
    }

    static Runnable helpPrinter(final String type)
    {
        return new Runnable()
        {
            @Override
            public void run()
            {
                printHelp(type);
            }
        };
    }

    static Runnable helpPrinter(final Command type)
    {
        return new Runnable()
        {
            @Override
            public void run()
            {
                printHelp(type);
            }
        };
    }
}

