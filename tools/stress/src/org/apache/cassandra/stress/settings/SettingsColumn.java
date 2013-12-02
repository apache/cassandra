package org.apache.cassandra.stress.settings;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.stress.generatedata.DataGenFactory;
import org.apache.cassandra.stress.generatedata.Distribution;
import org.apache.cassandra.stress.generatedata.DistributionFactory;
import org.apache.cassandra.stress.generatedata.DistributionFixed;
import org.apache.cassandra.stress.generatedata.RowGen;
import org.apache.cassandra.stress.generatedata.RowGenDistributedSize;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * For parsing column options
 */
public class SettingsColumn implements Serializable
{

    public final int maxColumnsPerKey;
    public final List<ByteBuffer> names;
    public final String comparator;
    public final boolean useTimeUUIDComparator;
    public final int superColumns;
    public final boolean useSuperColumns;
    public final boolean variableColumnCount;

    private final DistributionFactory sizeDistribution;
    private final DistributionFactory countDistribution;
    private final DataGenFactory dataGenFactory;

    public SettingsColumn(GroupedOptions options)
    {
        this((Options) options,
                options instanceof NameOptions ? (NameOptions) options : null,
                options instanceof CountOptions ? (CountOptions) options : null
        );
    }

    public SettingsColumn(Options options, NameOptions name, CountOptions count)
    {
        sizeDistribution = options.size.get();
        superColumns = Integer.parseInt(options.superColumns.value());
        dataGenFactory = options.generator.get();
        useSuperColumns = superColumns > 0;
        {
            comparator = options.comparator.value();
            AbstractType parsed = null;

            try
            {
                parsed = TypeParser.parse(comparator);
            }
            catch (Exception e)
            {
                System.err.println(e.getMessage());
                System.exit(1);
            }

            useTimeUUIDComparator = parsed instanceof TimeUUIDType;

            if (!(parsed instanceof TimeUUIDType || parsed instanceof AsciiType || parsed instanceof UTF8Type))
            {
                System.err.println("Currently supported types are: TimeUUIDType, AsciiType, UTF8Type.");
                System.exit(1);
            }
        }
        if (name != null)
        {
            assert count == null;

            AbstractType comparator;
            try
            {
                comparator = TypeParser.parse(this.comparator);
            } catch (Exception e)
            {
                throw new IllegalStateException(e);
            }

            final String[] names = name.name.value().split(",");
            this.names = new ArrayList<>(names.length);

            for (String columnName : names)
                this.names.add(comparator.fromString(columnName));

            final int nameCount = this.names.size();
            countDistribution = new DistributionFactory()
            {
                @Override
                public Distribution get()
                {
                    return new DistributionFixed(nameCount);
                }
            };
        }
        else
        {
            this.countDistribution = count.count.get();
            this.names = null;
        }
        maxColumnsPerKey = (int) countDistribution.get().maxValue();
        variableColumnCount = countDistribution.get().minValue() < maxColumnsPerKey;
    }

    public RowGen newRowGen()
    {
        return new RowGenDistributedSize(dataGenFactory.get(), countDistribution.get(), sizeDistribution.get());
    }

    // Option Declarations

    private static abstract class Options extends GroupedOptions
    {
        final OptionSimple superColumns = new OptionSimple("super=", "[0-9]+", "0", "Number of super columns to use (no super columns used if not specified)", false);
        final OptionSimple comparator = new OptionSimple("comparator=", "TimeUUIDType|AsciiType|UTF8Type", "AsciiType", "Column Comparator to use", false);
        final OptionDistribution size = new OptionDistribution("size=", "FIXED(34)");
        final OptionDataGen generator = new OptionDataGen("data=", "REPEAT(50)");
    }

    private static final class NameOptions extends Options
    {
        final OptionSimple name = new OptionSimple("names=", ".*", null, "Column names", true);

        @Override
        public List<? extends Option> options()
        {
            return Arrays.asList(name, superColumns, comparator, size, generator);
        }
    }

    private static final class CountOptions extends Options
    {
        final OptionDistribution count = new OptionDistribution("n=", "FIXED(5)");

        @Override
        public List<? extends Option> options()
        {
            return Arrays.asList(count, superColumns, comparator, size, generator);
        }
    }

    // CLI Utility Methods

    static SettingsColumn get(Map<String, String[]> clArgs)
    {
        String[] params = clArgs.remove("-col");
        if (params == null)
            return new SettingsColumn(new CountOptions());

        GroupedOptions options = GroupedOptions.select(params, new NameOptions(), new CountOptions());
        if (options == null)
        {
            printHelp();
            System.out.println("Invalid -col options provided, see output for valid options");
            System.exit(1);
        }
        return new SettingsColumn(options);
    }

    static void printHelp()
    {
        GroupedOptions.printOptions(System.out, "-col", new NameOptions(), new CountOptions());
    }

    static Runnable helpPrinter()
    {
        return new Runnable()
        {
            @Override
            public void run()
            {
                printHelp();
            }
        };
    }
}
