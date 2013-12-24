package org.apache.cassandra.stress.settings;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class SettingsPort implements Serializable
{

    public final int nativePort;
    public final int thriftPort;

    public SettingsPort(PortOptions options)
    {
        nativePort = Integer.parseInt(options.nativePort.value());
        thriftPort = Integer.parseInt(options.thriftPort.value());
    }

    // Option Declarations

    private static final class PortOptions extends GroupedOptions
    {
        final OptionSimple nativePort = new OptionSimple("native=", "[0-9]+", "9042", "Use this port for the Cassandra native protocol", false);
        final OptionSimple thriftPort = new OptionSimple("thrift=", "[0-9]+", "9160", "Use this port for the thrift protocol", false);

        @Override
        public List<? extends Option> options()
        {
            return Arrays.asList(nativePort, thriftPort);
        }
    }

    // CLI Utility Methods

    public static SettingsPort get(Map<String, String[]> clArgs)
    {
        String[] params = clArgs.remove("-port");
        if (params == null)
        {
            return new SettingsPort(new PortOptions());
        }
        PortOptions options = GroupedOptions.select(params, new PortOptions());
        if (options == null)
        {
            printHelp();
            System.out.println("Invalid -port options provided, see output for valid options");
            System.exit(1);
        }
        return new SettingsPort(options);
    }

    public static void printHelp()
    {
        GroupedOptions.printOptions(System.out, "-port", new PortOptions());
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

