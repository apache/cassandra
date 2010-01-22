package org.apache.cassandra.tools;

import java.io.IOException;

import org.apache.cassandra.db.CompactionManager;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

public class NodeCmd {
    private static final String HOST_OPTION = "host";
    private static final String PORT_OPTION = "port";
    private static final int defaultPort = 8080;
    private static Options options = null;
    
    static
    {
        options = new Options();
        Option optHost = new Option(HOST_OPTION, true, "node hostname or ip address");
        optHost.setRequired(true);
        options.addOption(optHost);
        options.addOption(PORT_OPTION, true, "remote jmx agent port number");
    }
    
    /**
     * Prints usage information to stdout.
     */
    private static void printUsage()
    {
        HelpFormatter hf = new HelpFormatter();
        String header = String.format(
                "%nAvailable commands: ring, info, cleanup, compact, cfstats, snapshot [name], clearsnapshot, " +
                "tpstats, flush, repair, decommission, move, loadbalance, removetoken, " +
                " getcompactionthreshold, setcompactionthreshold [minthreshold] ([maxthreshold])");
        String usage = String.format("java %s -host <arg> <command>%n", NodeProbe.class.getName());
        hf.printHelp(usage, "", options, header);
    }
    
    public static void main(String[] args) throws IOException, InterruptedException, ParseException
    {
        CommandLineParser parser = new PosixParser();
        CommandLine cmd = parser.parse(options, args);
        
        String host = cmd.getOptionValue(HOST_OPTION);
        int port = defaultPort;
        
        String portNum = cmd.getOptionValue(PORT_OPTION);
        if (portNum != null)
        {
            try
            {
                port = Integer.parseInt(portNum);
            }
            catch (NumberFormatException e)
            {
                throw new ParseException("Port must be a number");
            }
        }
        
        NodeProbe probe = null;
        try
        {
            probe = new NodeProbe(host, port);
        }
        catch (IOException ioe)
        {
            System.err.println("Error connecting to remote JMX agent!");
            ioe.printStackTrace();
            System.exit(3);
        }
        
        if (cmd.getArgs().length < 1)
        {
            System.err.println("Missing argument for command.");
            printUsage();
            System.exit(1);
        }
        
        // Execute the requested command.
        String[] arguments = cmd.getArgs();
        String cmdName = arguments[0];
        if (cmdName.equals("ring"))
        {
            probe.printRing(System.out);
        }
        else if (cmdName.equals("info"))
        {
            probe.printInfo(System.out);
        }
        else if (cmdName.equals("cleanup"))
        {
            probe.forceTableCleanup();
        }
        else if (cmdName.equals("compact"))
        {
            probe.forceTableCompaction();
        }
        else if (cmdName.equals("cfstats"))
        {
            probe.printColumnFamilyStats(System.out);
        }
        else if (cmdName.equals("decommission"))
        {
            probe.decommission();
        }
        else if (cmdName.equals("loadbalance"))
        {
            probe.loadBalance();
        }
        else if (cmdName.equals("move"))
        {
            if (arguments.length <= 1)
            {
                System.err.println("missing token argument");
            }
            probe.move(arguments[1]);
        }
        else if (cmdName.equals("removetoken"))
        {
            if (arguments.length <= 1)
            {
                System.err.println("missing token argument");
            }
            probe.removeToken(arguments[1]);
        }
        else if (cmdName.equals("snapshot"))
        {
            String snapshotName = "";
            if (arguments.length > 1)
            {
                snapshotName = arguments[1];
            }
            probe.takeSnapshot(snapshotName);
        }
        else if (cmdName.equals("clearsnapshot"))
        {
            probe.clearSnapshot();
        }
        else if (cmdName.equals("tpstats"))
        {
            probe.printThreadPoolStats(System.out);
        }
        else if (cmdName.equals("flush") || cmdName.equals("repair"))
        {
            if (cmd.getArgs().length < 2)
            {
                System.err.println("Missing keyspace argument.");
                printUsage();
                System.exit(1);
            }

            String[] columnFamilies = new String[cmd.getArgs().length - 2];
            for (int i = 0; i < columnFamilies.length; i++)
            {
                columnFamilies[i] = cmd.getArgs()[i + 2];
            }
            if (cmdName.equals("flush"))
                probe.forceTableFlush(cmd.getArgs()[1], columnFamilies);
            else // cmdName.equals("repair")
                probe.forceTableRepair(cmd.getArgs()[1], columnFamilies);
        }
        else if (cmdName.equals("getcompactionthreshold"))
        {
            probe.getCompactionThreshold(System.out);
        }
        else if (cmdName.equals("setcompactionthreshold"))
        {
            if (arguments.length < 2)
            {
                System.err.println("Missing threshold value(s)");
                printUsage();
                System.exit(1);
            }
            int minthreshold = Integer.parseInt(arguments[1]);
            int maxthreshold = CompactionManager.instance.getMaximumCompactionThreshold();
            if (arguments.length > 2)
            {
                maxthreshold = Integer.parseInt(arguments[2]);
            }

            if (minthreshold > maxthreshold)
            {
                System.err.println("Min threshold can't be greater than Max threshold");
                printUsage();
                System.exit(1);
            }

            if (minthreshold < 2 && maxthreshold != 0)
            {
                System.err.println("Min threshold must be at least 2");
                printUsage();
                System.exit(1);
            }
            probe.setCompactionThreshold(minthreshold, maxthreshold);
        }
        else
        {
            System.err.println("Unrecognized command: " + cmdName + ".");
            printUsage();
            System.exit(1);
        }

        System.exit(0);
    }
}
