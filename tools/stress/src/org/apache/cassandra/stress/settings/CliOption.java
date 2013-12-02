package org.apache.cassandra.stress.settings;

import java.util.HashMap;
import java.util.Map;

public enum CliOption
{
    KEY("Key details such as size in bytes and value distribution", SettingsKey.helpPrinter()),
    COL("Column details such as size and count distribution, data generator, names, comparator and if super columns should be used", SettingsColumn.helpPrinter()),
    RATE("Thread count, rate limit or automatic mode (default is auto)", SettingsRate.helpPrinter()),
    MODE("Thrift or CQL with options", SettingsMode.helpPrinter()),
    SCHEMA("Replication settings, compression, compaction, etc.", SettingsSchema.helpPrinter()),
    NODE("Nodes to connect to", SettingsNode.helpPrinter()),
    LOG("Where to log progress to, and the interval at which to do it", SettingsLog.helpPrinter()),
    TRANSPORT("Custom transport factories", SettingsTransport.helpPrinter()),
    PORT("The port to connect to cassandra nodes on", SettingsMisc.portHelpPrinter()),
    SENDTO("-send-to", "Specify a stress server to send this command to", SettingsMisc.sendToDaemonHelpPrinter())
    ;

    private static final Map<String, CliOption> LOOKUP;
    static
    {
        final Map<String, CliOption> lookup = new HashMap<>();
        for (CliOption cmd : values())
        {
            lookup.put("-" + cmd.toString().toLowerCase(), cmd);
            if (cmd.extraName != null)
                lookup.put(cmd.extraName, cmd);
        }
        LOOKUP = lookup;
    }

    public static CliOption get(String command)
    {
        return LOOKUP.get(command.toLowerCase());
    }

    public final String extraName;
    public final String description;
    private final Runnable helpPrinter;

    private CliOption(String description, Runnable helpPrinter)
    {
        this(null, description, helpPrinter);
    }
    private CliOption(String extraName, String description, Runnable helpPrinter)
    {
        this.extraName = extraName;
        this.description = description;
        this.helpPrinter = helpPrinter;
    }

    public void printHelp()
    {
        helpPrinter.run();
    }

}
