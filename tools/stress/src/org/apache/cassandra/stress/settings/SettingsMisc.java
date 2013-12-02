package org.apache.cassandra.stress.settings;

import org.apache.cassandra.stress.generatedata.Distribution;

import java.io.PrintStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class SettingsMisc implements Serializable
{

    static boolean maybeDoSpecial(Map<String, String[]> clArgs)
    {
        if (maybePrintHelp(clArgs))
            return true;
        if (maybePrintDistribution(clArgs))
            return true;
        return false;
    }

    static final class PrintDistribution extends GroupedOptions
    {
        final OptionDistribution dist = new OptionDistribution("dist=", null);

        @Override
        public List<? extends Option> options()
        {
            return Arrays.asList(dist);
        }
    }

    static boolean maybePrintDistribution(Map<String, String[]> clArgs)
    {
        final String[] args = clArgs.get("print");
        if (args == null)
            return false;
        final PrintDistribution dist = new PrintDistribution();
        if (null == GroupedOptions.select(args, dist))
        {
            printHelpPrinter().run();
            System.out.println("Invalid print options provided, see output for valid options");
            System.exit(1);
        }
        printDistribution(dist.dist.get().get());
        return true;
    }

    static void printDistribution(Distribution dist)
    {
        PrintStream out = System.out;
        out.println("% of samples    Range       % of total");
        String format = "%-16.1f%-12d%12.1f";
        double rangemax = dist.inverseCumProb(1d) / 100d;
        for (double d : new double[] { 0.1d, 0.2d, 0.3d, 0.4d, 0.5d, 0.6d, 0.7d, 0.8d, 0.9d, 0.95d, 0.99d, 1d })
        {
            double sampleperc = d * 100;
            long max = dist.inverseCumProb(d);
            double rangeperc = max/ rangemax;
            out.println(String.format(format, sampleperc, max, rangeperc));
        }
    }

    private static boolean maybePrintHelp(Map<String, String[]> clArgs)
    {
        if (!clArgs.containsKey("-?") && !clArgs.containsKey("help"))
            return false;
        String[] params = clArgs.remove("-?");
        if (params == null)
            params = clArgs.remove("help");
        if (params.length == 0)
        {
            if (!clArgs.isEmpty())
            {
                if (clArgs.size() == 1)
                {
                    String p = clArgs.keySet().iterator().next();
                    if (clArgs.get(p).length == 0)
                        params = new String[] {p};
                }
            }
            else
            {
                printHelp();
                return true;
            }
        }
        if (params.length == 1)
        {
            printHelp(params[0]);
            return true;
        }
        throw new IllegalArgumentException("Invalid command/option provided to help");
    }

    public static void printHelp()
    {
        System.out.println("Usage: ./bin/cassandra-stress <command> [options]");
        System.out.println();
        System.out.println("---Commands---");
        for (Command cmd : Command.values())
        {
            System.out.println(String.format("%-20s : %s", cmd.toString().toLowerCase(), cmd.description));
        }
        System.out.println();
        System.out.println("---Options---");
        for (CliOption cmd : CliOption.values())
        {
            System.out.println(String.format("-%-20s : %s", cmd.toString().toLowerCase(), cmd.description));
        }
    }

    public static void printHelp(String command)
    {
        Command cmd = Command.get(command);
        if (cmd != null)
        {
            cmd.printHelp();
            return;
        }
        CliOption opt = CliOption.get(command);
        if (opt != null)
        {
            opt.printHelp();
            return;
        }
        printHelp();
        throw new IllegalArgumentException("Invalid command or option provided to command help");
    }

    public static Runnable helpHelpPrinter()
    {
        return new Runnable()
        {
            @Override
            public void run()
            {
                System.out.println("Usage: ./bin/cassandra-stress help <command|option>");
                System.out.println("Commands:");
                for (Command cmd : Command.values())
                    System.out.println("    " + cmd.toString().toLowerCase() + (cmd.extraName != null ? ", " + cmd.extraName : ""));
                System.out.println("Options:");
                for (CliOption op : CliOption.values())
                    System.out.println("    -" + op.toString().toLowerCase() + (op.extraName != null ? ", " + op.extraName : ""));
            }
        };
    }

    public static Runnable printHelpPrinter()
    {
        return new Runnable()
        {
            @Override
            public void run()
            {
                GroupedOptions.printOptions(System.out, "print", new GroupedOptions()
                {
                    @Override
                    public List<? extends Option> options()
                    {
                        return Arrays.asList(new OptionDistribution("dist=", null));
                    }
                });
            }
        };
    }

    public static Runnable portHelpPrinter()
    {
        return new Runnable()
        {
            @Override
            public void run()
            {
                System.out.println("Usage: -port <port>");
                System.out.println();
                System.out.println("Specify the port of the cassandra server to connect to (default is 9160)");
            }
        };
    }

    public static Runnable sendToDaemonHelpPrinter()
    {
        return new Runnable()
        {
            @Override
            public void run()
            {
                System.out.println("Usage: -sendToDaemon <host>");
                System.out.println();
                System.out.println("Specify a host running the stress server to send this stress command to");
            }
        };
    }

    public static int getPort(Map<String, String[]> clArgs)
    {
        String[] params = clArgs.remove("-port");
        if (params == null)
            return 9160;
        if (params.length != 1)
        {
            portHelpPrinter().run();
            System.out.println("Invalid -port specifier: " + Arrays.toString(params));
            System.exit(1);
        }
        return Integer.parseInt(params[0]);

    }

    public static String getSendToDaemon(Map<String, String[]> clArgs)
    {
        String[] params = clArgs.remove("-send-to");
        if (params == null)
            params = clArgs.remove("-sendto");
        if (params == null)
            return null;
        if (params.length != 1)
        {
            sendToDaemonHelpPrinter().run();
            System.out.println("Invalid -send-to specifier: " + Arrays.toString(params));
            System.exit(1);
        }
        return params[0];

    }

}
