package org.apache.cassandra.stress.settings;

import com.datastax.driver.core.ProtocolOptions;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class SettingsMode implements Serializable
{

    public final ConnectionAPI api;
    public final ConnectionStyle style;
    public final CqlVersion cqlVersion;
    public final ProtocolOptions.Compression compression;

    public SettingsMode(GroupedOptions options)
    {
        if (options instanceof Cql3Options)
        {
            cqlVersion = CqlVersion.CQL3;
            Cql3Options opts = (Cql3Options) options;
            api = opts.useNative.setByUser() ? ConnectionAPI.JAVA_DRIVER_NATIVE : ConnectionAPI.THRIFT;
            style = opts.usePrepared.setByUser() ? ConnectionStyle.CQL_PREPARED : ConnectionStyle.CQL;
            compression = ProtocolOptions.Compression.valueOf(opts.useCompression.value().toUpperCase());
        }
        else if (options instanceof Cql3SimpleNativeOptions)
        {
            cqlVersion = CqlVersion.CQL3;
            Cql3SimpleNativeOptions opts = (Cql3SimpleNativeOptions) options;
            api = ConnectionAPI.SIMPLE_NATIVE;
            style = opts.usePrepared.setByUser() ? ConnectionStyle.CQL_PREPARED : ConnectionStyle.CQL;
            compression = ProtocolOptions.Compression.NONE;
        }
        else if (options instanceof Cql2Options)
        {
            cqlVersion = CqlVersion.CQL2;
            api = ConnectionAPI.THRIFT;
            Cql2Options opts = (Cql2Options) options;
            style = opts.usePrepared.setByUser() ? ConnectionStyle.CQL_PREPARED : ConnectionStyle.CQL;
            compression = ProtocolOptions.Compression.NONE;
        }
        else if (options instanceof ThriftOptions)
        {
            ThriftOptions opts = (ThriftOptions) options;
            cqlVersion = CqlVersion.NOCQL;
            api = opts.smart.setByUser() ? ConnectionAPI.THRIFT_SMART : ConnectionAPI.THRIFT;
            style = ConnectionStyle.THRIFT;
            compression = ProtocolOptions.Compression.NONE;
        }
        else
            throw new IllegalStateException();
    }

    // Option Declarations

    private static final class Cql3Options extends GroupedOptions
    {
        final OptionSimple api = new OptionSimple("cql3", "", null, "", true);
        final OptionSimple useNative = new OptionSimple("native", "", null, "", false);
        final OptionSimple usePrepared = new OptionSimple("prepared", "", null, "", false);
        final OptionSimple useCompression = new OptionSimple("compression=", "none|lz4|snappy", "none", "", false);
        final OptionSimple port = new OptionSimple("port=", "[0-9]+", "9046", "", false);

        @Override
        public List<? extends Option> options()
        {
            return Arrays.asList(useNative, usePrepared, api, useCompression, port);
        }
    }

    private static final class Cql3SimpleNativeOptions extends GroupedOptions
    {
        final OptionSimple api = new OptionSimple("cql3", "", null, "", true);
        final OptionSimple useSimpleNative = new OptionSimple("simplenative", "", null, "", true);
        final OptionSimple usePrepared = new OptionSimple("prepared", "", null, "", false);
        final OptionSimple port = new OptionSimple("port=", "[0-9]+", "9046", "", false);

        @Override
        public List<? extends Option> options()
        {
            return Arrays.asList(useSimpleNative, usePrepared, api, port);
        }
    }

    private static final class Cql2Options extends GroupedOptions
    {
        final OptionSimple api = new OptionSimple("cql2", "", null, "", true);
        final OptionSimple usePrepared = new OptionSimple("prepared", "", null, "", false);

        @Override
        public List<? extends Option> options()
        {
            return Arrays.asList(usePrepared, api);
        }
    }

    private static final class ThriftOptions extends GroupedOptions
    {
        final OptionSimple api = new OptionSimple("thrift", "", null, "", true);
        final OptionSimple smart = new OptionSimple("smart", "", null, "", false);

        @Override
        public List<? extends Option> options()
        {
            return Arrays.asList(api, smart);
        }
    }

    // CLI Utility Methods

    public static SettingsMode get(Map<String, String[]> clArgs)
    {
        String[] params = clArgs.remove("-mode");
        if (params == null)
        {
            ThriftOptions opts = new ThriftOptions();
            opts.smart.accept("smart");
            return new SettingsMode(opts);
        }

        GroupedOptions options = GroupedOptions.select(params, new ThriftOptions(), new Cql2Options(), new Cql3Options(), new Cql3SimpleNativeOptions());
        if (options == null)
        {
            printHelp();
            System.out.println("Invalid -mode options provided, see output for valid options");
            System.exit(1);
        }
        return new SettingsMode(options);
    }

    public static void printHelp()
    {
        GroupedOptions.printOptions(System.out, "-mode", new ThriftOptions(), new Cql2Options(), new Cql3Options(), new Cql3SimpleNativeOptions());
    }

    public static Runnable helpPrinter()
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
