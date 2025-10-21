/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.tools.nodetool.layout;

import org.apache.cassandra.tools.nodetool.CommandUtils;
import org.apache.cassandra.tools.nodetool.JmxConnect;
import org.apache.cassandra.tools.nodetool.NodetoolCommand;
import org.apache.cassandra.utils.Pair;
import org.apache.commons.lang3.StringUtils;
import picocli.CommandLine;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.cassandra.tools.nodetool.CommandUtils.findCassandraBackwardCompatibleArgument;
import static org.apache.cassandra.tools.nodetool.CommandUtils.sortShortestFirst;
import static org.apache.commons.lang3.ArrayUtils.isEmpty;
import static picocli.CommandLine.Model.UsageMessageSpec.SECTION_KEY_COMMAND_LIST;
import static picocli.CommandLine.Model.UsageMessageSpec.SECTION_KEY_COMMAND_LIST_HEADING;
import static picocli.CommandLine.Model.UsageMessageSpec.SECTION_KEY_DESCRIPTION;
import static picocli.CommandLine.Model.UsageMessageSpec.SECTION_KEY_DESCRIPTION_HEADING;
import static picocli.CommandLine.Model.UsageMessageSpec.SECTION_KEY_END_OF_OPTIONS;
import static picocli.CommandLine.Model.UsageMessageSpec.SECTION_KEY_EXIT_CODE_LIST;
import static picocli.CommandLine.Model.UsageMessageSpec.SECTION_KEY_EXIT_CODE_LIST_HEADING;
import static picocli.CommandLine.Model.UsageMessageSpec.SECTION_KEY_FOOTER;
import static picocli.CommandLine.Model.UsageMessageSpec.SECTION_KEY_FOOTER_HEADING;
import static picocli.CommandLine.Model.UsageMessageSpec.SECTION_KEY_HEADER;
import static picocli.CommandLine.Model.UsageMessageSpec.SECTION_KEY_HEADER_HEADING;
import static picocli.CommandLine.Model.UsageMessageSpec.SECTION_KEY_OPTION_LIST;
import static picocli.CommandLine.Model.UsageMessageSpec.SECTION_KEY_OPTION_LIST_HEADING;
import static picocli.CommandLine.Model.UsageMessageSpec.SECTION_KEY_PARAMETER_LIST;
import static picocli.CommandLine.Model.UsageMessageSpec.SECTION_KEY_PARAMETER_LIST_HEADING;
import static picocli.CommandLine.Model.UsageMessageSpec.SECTION_KEY_SYNOPSIS;
import static picocli.CommandLine.Model.UsageMessageSpec.SECTION_KEY_SYNOPSIS_HEADING;

/**
 * Help factory for the Cassandra nodetool to generate the help output in the format
 * of the airline help output, which is used as the default layout for the Cassandra nodetool.
 * <p>
 * Note, that JMX connect options are always shown in the help output and are not hidden. The
 * {@link JmxConnect} class is used to connect to a C* node via JMX, but the opttions are not
 * part of the command hierarchy to allow reusage of the commands in other contexts.
 */
public class CassandraCliHelpLayout extends CommandLine.Help
{
    // The default width for the usage help output to match the width of
    // the airline help output and minimize the divergence of layouts.
    public static final int DEFAULT_USAGE_HELP_WIDTH = 88;
    private static final int TOP_LEVEL_USAGE_HELP_WIDTH = 999;
    private static final String DESCRIPTION_HEADING = "NAME%n";
    private static final String SYNOPSIS_HEADING = "SYNOPSIS%n";
    private static final String OPTIONS_HEADING = "OPTIONS%n";
    private static final String COMMANDS_HEADING = "COMMANDS%n";
    private static final String FOOTER_HEADING = "%n";
    private static final String SINGLE_SPACE = " ";
    private static final int DESCRIPTION_INDENT = 4;
    public static final int COLUMN_INDENT = 8;
    public static final int SUBCOMMANDS_INDENT = 4;
    public static final int SUBCOMMANDS_DESCRIPTION_INDENT_TOP_LEVEL = 3;
    private static final CommandLine.Model.OptionSpec CASSANDRA_END_OF_OPTIONS_OPTION =
        CommandLine.Model.OptionSpec.builder("--")
                                    .description("This option can be used to separate command-line options from the " +
                                                 "list of argument, (useful when arguments might be mistaken for " +
                                                 "command-line options")
                                    .arity("0")
                                    .build();
    public static final String TOP_LEVEL_SYNOPSIS_LIST_PREFIX = "usage:";
    public static final String TOP_LEVEL_COMMAND_HEADING = "The most commonly used nodetool commands are:";
    public static final String USAGE_HELP_FOOTER = "See 'nodetool help <command>' for more information on a specific command.";
    public static final String SYNOPSIS_SUBCOMMANDS_LABEL = "<command> [<args>]";
    public static final String SUBCOMMAND_OPTION_TEMPLATE = "With %s option, %s";
    public static final String SUBCOMMAND_SUBHEADER = "With no arguments, Display help information";
    private static final String[] EMPTY_FOOTER = new String[0];

    public CassandraCliHelpLayout(CommandLine.Model.CommandSpec spec, ColorScheme scheme)
    {
        super(spec, scheme);
    }

    @Override
    public String descriptionHeading(Object... params)
    {
        return createHeading(DESCRIPTION_HEADING, params);
    }

    /**
     * @param params Arguments referenced by the format specifiers in the header strings
     * @return the header string.
     */
    @Override
    public String description(Object... params)
    {
        CommandLine.Model.CommandSpec spec = commandSpec();
        String fullName = spec.qualifiedName();

        TextTable table = TextTable.forColumns(colorScheme(),
                                               new Column(spec.usageMessage().width() - COLUMN_INDENT, COLUMN_INDENT,
                                                          Column.Overflow.WRAP));
        table.setAdjustLineBreaksForWideCJKCharacters(spec.usageMessage().adjustLineBreaksForWideCJKCharacters());
        table.indentWrappedLines = 0;

        table.addRowValues(colorScheme().commandText(fullName)
                                        .concat(" - ")
                                        .concat(colorScheme().text(String.join(" ", spec.usageMessage().description()))));
        table.addRowValues(Ansi.OFF.new Text("", colorScheme()));
        return table.toString(new StringBuilder()).toString();
    }

    @Override
    public String synopsisHeading(Object... params)
    {
        return createHeading(SYNOPSIS_HEADING, params);
    }

    /**
     * This method is overridden to provide a detailed synopsis for the command and its subcommands.
     * <pre>
     * {@code
     *     SYNOPSIS
     *         nodetool [(-h <host> | --host <host>)] [(-p <port> | --port <port>)]
     *                 [(-pp | --print-port)] [(-pw <password> | --password <password>)]
     *                 [(-pwf <passwordFilePath> | --password-file <passwordFilePath>)]
     *                 [(-u <username> | --username <username>)] bootstrap <command>
     *                 [<args>]
     * }
     * </pre>
     *
     * @param synopsisHeadingLength the length of the synopsis heading that will be displayed on the same line
     * @return The synopsis string.
     */
    @Override
    public String synopsis(int synopsisHeadingLength)
    {
        StringBuilder top = new StringBuilder(printDetailedSynopsis(commandSpec(), "", true));
        for (CommandLine sub : commandSpec().subcommands().values())
            top.append(printDetailedSynopsis(sub.getCommandSpec(), "", true));
        return top.toString();
    }

    private String printDetailedSynopsis(CommandLine.Model.CommandSpec commandSpec,
                                         String synopsisPrefix,
                                         boolean showEndOfOptionsDelimiter)
    {
        // Cassandra uses end of options delimiter in usage help.
        commandSpec.usageMessage().showEndOfOptionsDelimiterInUsageHelp(showEndOfOptionsDelimiter);

        ColorScheme colorScheme = colorScheme();

        List<CommandLine.Model.OptionSpec> parentOptions = parentCommandOptionsWithJmxOptions(commandSpec);
        List<CommandLine.Model.OptionSpec> commandOptions = commandSpec.options();
        // Retain only the options that are not part of the command hierarchy (e.g. dynamic options).
        Comparator<CommandLine.Model.OptionSpec> comparator = new OptionSpecByNamesComparator();
        parentOptions.removeIf(parentOpt ->
                commandOptions.stream().anyMatch(commandOpt -> comparator.compare(parentOpt, commandOpt) == 0));

        List<Ansi.Text> parentOptionsList = createCassandraSynopsisOptionsText(parentOptions);
        List<Ansi.Text> commandOptionsList = createCassandraSynopsisOptionsText(commandOptions);

        Ansi.Text positionalParamText = createCassandraSynopsisPositionalsText(commandSpec, colorScheme);
        Ansi.Text endOfOptionsText = positionalParamText.plainString().isEmpty() ?
                                     colorScheme.text("") :
                                     colorScheme.text("[")
                                                .concat(commandSpec.parser().endOfOptionsDelimiter())
                                                .concat("]");

        Ansi.Text commandText = ansi().new Text(0);
        if (!commandSpec.subcommands().isEmpty())
            commandText = commandText.concat(SYNOPSIS_SUBCOMMANDS_LABEL);

        int usageHelpWidth = commandSpec.usageMessage().width();
        boolean isEmptyParent = commandSpec.root() == commandSpec;
        Ansi.Text rootCommandText = colorScheme.commandText(commandSpec.root().name());
        // If the command is the top-level command, use the command name as the root command text.
        // Otherwise, use the fully qualified command name without the root command name.
        // Example: "nodetool status" -> "status", "nodetool status thrift" -> "status thrift"
        Ansi.Text mainCommandText = isEmptyParent ? colorScheme.commandText(commandSpec.name()) :
                                    colorScheme.commandText(commandSpec.qualifiedName().replace(rootCommandText.plainString(), "").trim());
        TextTable textTable = TextTable.forColumns(colorScheme, new Column(usageHelpWidth, COLUMN_INDENT, Column.Overflow.WRAP));
        textTable.indentWrappedLines = COLUMN_INDENT;
        textTable.setAdjustLineBreaksForWideCJKCharacters(commandSpec.usageMessage().adjustLineBreaksForWideCJKCharacters());

        // Consider the following example:
        // SYNOPSIS
        //         nodetool [(-h <host> | --host <host>)] [(-p <port> | --port <port>)]
        //                [(-pw <password> | --password <password>)]
        //                [(-pwf <passwordFilePath> | --password-file <passwordFilePath>)]
        //                [(-u <username> | --username <username>)] describecluster
        //                [(-pp | --print-port)]
        new LineBreakingLayout(colorScheme, usageHelpWidth, textTable)
            .concatItem(synopsisPrefix.isEmpty() ? rootCommandText : colorScheme.text(synopsisPrefix).concat(" ").concat(rootCommandText))
            // Print "[(-h <host> | --host <host>)] [(-p <port> | --port <port>)]" options related to the parent command.
            .concatItems(parentOptionsList)
            // Print "describecluster" in the same line as the parent options.
            .concatItem(isEmptyParent ? colorScheme.text("") : mainCommandText)
            // Print "[(-pp | --print-port)]" options related to the command itself.
            .concatItems(commandOptionsList)
            .concatItem(endOfOptionsText)
            // All other fields added to the synopsis are left-adjusted, so we don't need to add them one by one.
            .flush(positionalParamText.concat(commandText));

        textTable.addEmptyRow();
        return textTable.toString();
    }

    private static Ansi.Text createCassandraSynopsisPositionalsText(CommandLine.Model.CommandSpec spec,
                                                                    ColorScheme colorScheme)
    {
        List<CommandLine.Model.PositionalParamSpec> positionals = cassandraPositionals(spec);

        Pair<String, String> commandArgumensSpec = findCassandraBackwardCompatibleArgument(spec.userObject());
        Ansi.Text text = colorScheme.text("");
        // If the command has a backward compatible @CassandraUsage argument,
        // use it to generate the synopsis based on the old format.
        if (commandArgumensSpec != null)
            return colorScheme.parameterText(commandArgumensSpec.left);

        IParamLabelRenderer parameterLabelRenderer = CassandraStyleParamLabelRender.create();
        for (CommandLine.Model.PositionalParamSpec positionalParam : positionals)
        {
            Ansi.Text label = parameterLabelRenderer.renderParameterLabel(positionalParam, colorScheme.ansi(), colorScheme.parameterStyles());
            text = text.plainString().isEmpty() ? label : text.concat(" ").concat(label);
        }
        return text;
    }

    private static List<CommandLine.Model.OptionSpec> parentCommandOptionsWithJmxOptions(final CommandLine.Model.CommandSpec commandSpec)
    {
        // If the command is the help local command, no need to show the parent options.
        if (commandSpec.helpCommand())
            return Collections.emptyList();

        List<CommandLine.Model.CommandSpec> hierarhy = new LinkedList<>();
        CommandLine.Model.CommandSpec curr;
        CommandLine.Model.CommandSpec command = commandSpec;
        while ((curr = command.parent()) != null)
        {
            hierarhy.add(curr);
            command = curr;
        }
        Collections.reverse(hierarhy);
        List<CommandLine.Model.OptionSpec> options = new ArrayList<>();
        for (CommandLine.Model.CommandSpec spec : hierarhy)
        {
            for (CommandLine.Model.OptionSpec option : spec.options())
            {
                // JmxConnect and NodetoolCommand options are always shown in the help output for backwards compatibility.
                if (option.userObject() instanceof Field &&
                    (((Field) option.userObject()).getDeclaringClass().equals(JmxConnect.class) ||
                     ((Field) option.userObject()).getDeclaringClass().equals(NodetoolCommand.class)))
                    options.add(option);
                else
                {
                    if (option.hidden() || option.scopeType() == CommandLine.ScopeType.LOCAL)
                        continue;
                    options.add(option);
                }
            }
        }
        return options;
    }

    private List<Ansi.Text> createCassandraSynopsisOptionsText(List<CommandLine.Model.OptionSpec> options)
    {
        // Cassandra uses alphabetical order for options, ordered by short name.
        List<CommandLine.Model.OptionSpec> optionList = new ArrayList<>(options);
        optionList.sort(createShortOptionNameComparator());
        List<Ansi.Text> result = new ArrayList<>();

        ColorScheme colorScheme = colorScheme();
        IParamLabelRenderer parameterLabelRenderer = CassandraStyleParamLabelRender.create();

        for (CommandLine.Model.OptionSpec option : optionList)
        {
            if (option.hidden())
                continue;

            Ansi.Text text = ansi().new Text(0);
            String[] names = sortShortestFirst(option.names());
            if (names.length == 1)
            {
                text = text.concat("[").concat(colorScheme.optionText(names[0]))
                           .concat(spacedParamLabel(option, parameterLabelRenderer, colorScheme))
                           .concat("]");
            }
            else
            {
                Ansi.Text shortName = colorScheme.optionText(option.shortestName());
                Ansi.Text fullName = colorScheme.optionText(option.longestName());
                boolean isArrayOrCollection =
                    option.userObject() instanceof Field
                    && (((Field) option.userObject()).getType().isArray() ||
                        Collection.class.isAssignableFrom(((Field) option.userObject()).getType()));
                text = text.concat("[(")
                           .concat(shortName)
                           .concat(spacedParamLabel(option, parameterLabelRenderer, colorScheme))
                           .concat(" | ")
                           .concat(fullName)
                           .concat(spacedParamLabel(option, parameterLabelRenderer, colorScheme))
                           .concat(isArrayOrCollection ? ")...]" : ")]");
            }

            result.add(text);
        }
        return result;
    }

    @Override
    public String optionListHeading(Object... params)
    {
        return createHeading(OPTIONS_HEADING, params);
    }

    /**
     * Returns the help for the options of the current command.
     * <pre>
     * {@code
     * OPTIONS
     *         --help
     *             Show usage help for the help command and exit.
     *
     *         --
     *             This option can be used to separate command-line options from
     *             the list of argument, (useful when arguments might be mistaken
     *             for command-line options
     * }
     * </pre>
     * @return The string representation of the options.
     */
    @Override
    public String optionList()
    {
        CommandLine.Model.CommandSpec spec = commandSpec();
        List<CommandLine.Model.OptionSpec> commandOptions = spec.options();
        List<CommandLine.Model.OptionSpec> parentOptions = parentCommandOptionsWithJmxOptions(spec);
        Comparator<CommandLine.Model.OptionSpec> comparator = new OptionSpecByNamesComparator();
        parentOptions.removeIf(parentOpt ->
                commandOptions.stream().anyMatch(commandOpt -> comparator.compare(parentOpt, commandOpt) == 0));

        List<CommandLine.Model.OptionSpec> uniqueOptions = Stream.concat(parentOptions.stream(),
                                                                         commandOptions.stream().filter(o -> !o.hidden()))
                                                                 .distinct()
                                                                 .sorted(createShortOptionNameComparator())
                                                                 .collect(Collectors.toList());

        Layout layout = cassandraSingleColumnOptionsParametersLayout();
        layout.addAllOptions(uniqueOptions, CassandraStyleParamLabelRender.create());
        return layout.toString();
    }

    @Override
    public String endOfOptionsList()
    {
        List<CommandLine.Model.PositionalParamSpec> positionals = cassandraPositionals(commandSpec());
        if (positionals.isEmpty())
            return "";
        Layout layout = cassandraSingleColumnOptionsParametersLayout();
        layout.addOption(CASSANDRA_END_OF_OPTIONS_OPTION, CassandraStyleParamLabelRender.create());
        return layout.toString();
    }

    private Layout cassandraSingleColumnOptionsParametersLayout()
    {
        return new Layout(colorScheme(), configureLayoutTextTable(), new CassandraStyleOptionRenderer(), new CassandraStyleParameterRenderer());
    }

    private TextTable configureLayoutTextTable()
    {
        TextTable table = TextTable.forColumns(colorScheme(), new Column(commandSpec().usageMessage().width() - COLUMN_INDENT,
                                                                         COLUMN_INDENT, Column.Overflow.WRAP));
        table.setAdjustLineBreaksForWideCJKCharacters(commandSpec().usageMessage().adjustLineBreaksForWideCJKCharacters());
        table.indentWrappedLines = DESCRIPTION_INDENT;
        return table;
    }

    @Override
    public String parameterList()
    {
        Pair<String, String> cassandraArgument = findCassandraBackwardCompatibleArgument(commandSpec().userObject());
        List<CommandLine.Model.PositionalParamSpec> positionalParams = cassandraPositionals(commandSpec());
        TextTable table = configureLayoutTextTable();
        Layout layout = cassandraArgument == null ?
                        cassandraSingleColumnOptionsParametersLayout() :
                        new Layout(colorScheme(),
                                   table,
                                   new CassandraStyleOptionRenderer(),
                                   new CassandraStyleParameterRenderer())
                        {
                            // If the command has a backward compatible argument, use it to generate the synopsis
                            // based on the old format.
                            @Override
                            public void layout(CommandLine.Model.ArgSpec argSpec, Ansi.Text[][] cellValues)
                            {
                                Ansi.Text descPadding = Ansi.OFF.new Text(leadingSpaces(DESCRIPTION_INDENT), colorScheme);
                                cellValues[0] = new Ansi.Text[]{ colorScheme.parameterText(cassandraArgument.left) };
                                cellValues[1] = new Ansi.Text[]{ descPadding.concat(colorScheme.parameterText(cassandraArgument.right)) };
                                cellValues[2] = new Ansi.Text[]{ Ansi.OFF.new Text("", colorScheme) };
                                for (Ansi.Text[] oneRow : cellValues)
                                    table.addRowValues(oneRow);
                            }

                            @Override
                            public void addAllPositionalParameters(List<CommandLine.Model.PositionalParamSpec> params,
                                                                   IParamLabelRenderer paramLabelRenderer)
                            {
                                layout(null, new Ansi.Text[3][]);
                            }
                        };

        layout.addAllPositionalParameters(positionalParams, CassandraStyleParamLabelRender.create());
        table.addEmptyRow();
        return layout.toString();
    }

    @Override
    public String commandListHeading(Object... params)
    {
        if (commandSpec().subcommands().isEmpty())
            return "";
        return createHeading(COMMANDS_HEADING, params);
    }

    /**
     * Returns the help for the subcommands of the current command.
     * <pre>
     * {@code
     * COMMANDS
     *         With no arguments, Display help information
     *
     *         resume
     *             Resume bootstrap streaming
     *
     *             With --force option, Use --force to resume bootstrap regardless of
     *             cassandra.reset_bootstrap_progress environment variable. WARNING: This
     *             is potentially dangerous, see CASSANDRA-17679
     * }
     * </pre>
     * @param subcommands The subcommands of the current command.
     * @return The string representation of the subcommands.
     */
    @Override
    public String commandList(Map<String, CommandLine.Help> subcommands)
    {
        if (subcommands.isEmpty())
            return "";
        TextTable table = TextTable.forColumns(colorScheme(), new Column(commandSpec().usageMessage().width(),
                                                                         COLUMN_INDENT, Column.Overflow.WRAP));
        table.indentWrappedLines = SUBCOMMANDS_INDENT;
        table.setAdjustLineBreaksForWideCJKCharacters(commandSpec().usageMessage().adjustLineBreaksForWideCJKCharacters());
        table.addRowValues(colorScheme().parameterText(SUBCOMMAND_SUBHEADER));
        table.addEmptyRow();

        for (Map.Entry<String, CommandLine.Help> entry : subcommands.entrySet())
        {
            CommandLine.Help help = entry.getValue();
            CommandLine.Model.UsageMessageSpec usage = help.commandSpec().usageMessage();
            String header = isEmpty(usage.header()) ? (isEmpty(usage.description()) ? "" : usage.description()[0]) : usage.header()[0];
            table.addRowValues(colorScheme().commandText(entry.getKey()));

            Ansi.Text leadingSpaces = Ansi.OFF.new Text(leadingSpaces(SUBCOMMANDS_INDENT), colorScheme());
            table.addRowValues(leadingSpaces.concat(colorScheme().text(header)));

            List<CommandLine.Model.OptionSpec> optionSpecs = help.commandSpec().options();
            for (CommandLine.Model.OptionSpec optionSpec : optionSpecs)
            {
                if (optionSpec.hidden())
                    continue;
                table.addEmptyRow();

                // Print the option description in multiple lines as it is set in the annotation.
                for (int i = 0; i < optionSpec.description().length; i++)
                {
                    if (i == 0)
                    {
                        table.addRowValues(leadingSpaces.concat(
                            colorScheme().optionText(String.format(SUBCOMMAND_OPTION_TEMPLATE,
                                                                   optionSpec.longestName(),
                                                                   colorScheme().text(optionSpec.description()[i])))));
                    }
                    else
                    {
                        table.addRowValues(leadingSpaces.concat(colorScheme().text(optionSpec.description()[i])));
                    }
                }
            }
        }
        return table.toString();
    }

    @Override
    public String footerHeading(Object... params)
    {
        return createHeading(FOOTER_HEADING, params);
    }

    @Override
    public String footer(Object... params)
    {
        String[] footer;
        if (commandSpec().parent() == null)
            footer = isEmpty(commandSpec().usageMessage().footer()) ? new String[]{ USAGE_HELP_FOOTER } :
                     commandSpec().usageMessage().footer();
        else
            footer = EMPTY_FOOTER;

        TextTable table = TextTable.forColumns(colorScheme(), new Column(commandSpec().usageMessage().width(), 0, Column.Overflow.WRAP));
        table.setAdjustLineBreaksForWideCJKCharacters(commandSpec().usageMessage().adjustLineBreaksForWideCJKCharacters());
        table.indentWrappedLines = 0;

        for (String summaryLine : footer)
            table.addRowValues(String.format(summaryLine, params));
        table.addEmptyRow();
        return table.toString();
    }

    public String topLevelCommandListHeading(Object... params)
    {
        return createHeading(TOP_LEVEL_COMMAND_HEADING + "%n", params);
    }

    public String topLevelSynopsis(CommandLine.Help help)
    {
        return printDetailedSynopsis(commandSpec(), TOP_LEVEL_SYNOPSIS_LIST_PREFIX, false);
    }

    /**
     * Returns the help for the top-level command. This differs from the {@link #commandList(Map)} method
     * in that it does not include the subcommands.
     * <pre>
     * {@code
     * The most commonly used nodetool commands are:
     *     abortbootstrap                      Abort a failed bootstrap
     *     bootstrap                           Monitor/manage node's bootstrap process
     *     cidrfilteringstats                  Print statistics on CIDR filtering
     *     clientstats                         Print information about connected clients
     * }
     * </pre>
     * @param help The help object to use for rendering the command list.
     * @return The top-level subcommands list.
     */
    public String topLevelCommandList(CommandLine.Help help)
    {
        Map<String, CommandLine.Help> subcommands = new TreeMap<>(commandSpec().commandLine().getHelp().subcommands());
        int width = TOP_LEVEL_USAGE_HELP_WIDTH;
        int commandLength = Math.min(CommandUtils.maxLength(subcommands.keySet()), width / 2);
        int leadinColumnWidth = commandLength + SUBCOMMANDS_INDENT;
        TextTable table = TextTable.forColumns(colorScheme(),
                                               new Column(leadinColumnWidth, SUBCOMMANDS_INDENT, Column.Overflow.SPAN),
                                               new Column(width - leadinColumnWidth, SUBCOMMANDS_DESCRIPTION_INDENT_TOP_LEVEL, Column.Overflow.TRUNCATE));
        table.setAdjustLineBreaksForWideCJKCharacters(commandSpec().usageMessage().adjustLineBreaksForWideCJKCharacters());

        for (Map.Entry<String, CommandLine.Help> entry : subcommands.entrySet())
        {
            CommandLine.Help helpSubcommand = entry.getValue();
            CommandLine.Model.UsageMessageSpec usage = helpSubcommand.commandSpec().usageMessage();
            String header = isEmpty(usage.header()) ? (isEmpty(usage.description()) ? "" : usage.description()[0]) : usage.header()[0];
            Ansi.Text[] lines = colorScheme().text(header).splitLines();
            for (int i = 0; i < lines.length; i++)
                table.addRowValues(i == 0 ? colorScheme().commandText(entry.getKey()) : Ansi.OFF.new Text(0), lines[i]);
        }
        return table.toString();
    }

    private static List<CommandLine.Model.PositionalParamSpec> cassandraPositionals(CommandLine.Model.CommandSpec commandSpec)
    {
        List<CommandLine.Model.PositionalParamSpec> positionals = new ArrayList<>(commandSpec.positionalParameters());
        positionals.removeIf(CommandLine.Model.ArgSpec::hidden);
        return positionals;
    }

    /**
     * Layout for cassandra help CLI output.
     * @return List of keys for the help sections.
     */
    public static List<String> cassandraHelpSectionKeys()
    {
        List<String> result = new LinkedList<>();
        result.add(SECTION_KEY_HEADER_HEADING);
        result.add(SECTION_KEY_HEADER);
        result.add(SECTION_KEY_DESCRIPTION_HEADING);
        result.add(SECTION_KEY_DESCRIPTION);
        result.add(SECTION_KEY_SYNOPSIS_HEADING);
        result.add(SECTION_KEY_SYNOPSIS);
        result.add(SECTION_KEY_OPTION_LIST_HEADING);
        result.add(SECTION_KEY_OPTION_LIST);
        result.add(SECTION_KEY_END_OF_OPTIONS);
        result.add(SECTION_KEY_PARAMETER_LIST_HEADING);
        result.add(SECTION_KEY_PARAMETER_LIST);
        result.add(SECTION_KEY_COMMAND_LIST_HEADING);
        result.add(SECTION_KEY_COMMAND_LIST);
        result.add(SECTION_KEY_EXIT_CODE_LIST_HEADING);
        result.add(SECTION_KEY_EXIT_CODE_LIST);
        result.add(SECTION_KEY_FOOTER_HEADING);
        result.add(SECTION_KEY_FOOTER);
        return result;
    }

    /**
     * Returns a string with the given number of leading spaces.
     *
     * @param num the number of leading spaces
     * @return the string with the given number of leading spaces
     */
    private static String leadingSpaces(int num)
    {
        return SINGLE_SPACE.repeat(num);
    }

    private static Ansi.Text spacedParamLabel(CommandLine.Model.OptionSpec optionSpec,
                                       IParamLabelRenderer parameterLabelRenderer,
                                       ColorScheme scheme)
    {
        return optionSpec.typeInfo().isBoolean() ? scheme.text("") :
               scheme.text(" ").concat(parameterLabelRenderer.renderParameterLabel(optionSpec, scheme.ansi(), scheme.optionParamStyles()));
    }

    private static class CassandraStyleParamLabelRender implements IParamLabelRenderer
    {
        public static IParamLabelRenderer create()
        {
            return new CassandraStyleParamLabelRender();
        }

        @Override
        public Ansi.Text renderParameterLabel(CommandLine.Model.ArgSpec argSpec, Ansi ansi, List<Ansi.IStyle> styles)
        {
            ColorScheme colorScheme = CommandLine.Help.defaultColorScheme(ansi);
            if (argSpec.equals(CASSANDRA_END_OF_OPTIONS_OPTION))
                return colorScheme.text("");
            if (argSpec instanceof CommandLine.Model.OptionSpec && argSpec.typeInfo().isBoolean())
                return colorScheme.text("");

            if (argSpec.paramLabel().contains(" "))
                throw new IllegalArgumentException("Spaces are not allowed in paramLabel: " + argSpec.paramLabel());
            return argSpec.isOption() ? colorScheme.optionText(renderParamLabel(argSpec)) :
                   colorScheme.parameterText(renderParamLabel(argSpec));
        }

        private static String renderParamLabel(CommandLine.Model.ArgSpec argSpec)
        {
            if (argSpec.userObject() instanceof Field)
            {
                Field field = (Field) argSpec.userObject();
                if (StringUtils.isEmpty(argSpec.paramLabel()))
                    return field.getName();
                String label = argSpec.paramLabel().replace("<", "").replace(">", "");
                return '<' + label + '>';
            }
            return argSpec.paramLabel();
        }

        @Override
        public String separator() { return ""; }
    }

    private static class CassandraStyleOptionRenderer implements IOptionRenderer
    {
        @Override
        public Ansi.Text[][] render(CommandLine.Model.OptionSpec option, IParamLabelRenderer parameterLabelRenderer, ColorScheme scheme)
        {
            Ansi.Text optionText = scheme.optionText("");
            for (int i = 0; i < option.names().length; i++)
            {
                String name = option.names()[i];
                optionText = optionText.concat(scheme.optionText(name))
                                       .concat(spacedParamLabel(option, parameterLabelRenderer, scheme))
                                       .concat(i == option.names().length - 1 ? "" : ", ");
            }

            Ansi.Text descPadding = Ansi.OFF.new Text(leadingSpaces(DESCRIPTION_INDENT), scheme);
            String[] description = option.description().length == 0 ? new String[]{ "" } : option.description();
            // header, descriptions [0..*], empty line
            int height = 2 + description.length;
            Ansi.Text[][] result = new Ansi.Text[height][];
            result[0] = new Ansi.Text[]{ optionText };
            for (int i = 0; i < description.length; i++)
                result[i + 1] = new Ansi.Text[]{ descPadding.concat(scheme.text(description[i])) };
            result[height - 1] = new Ansi.Text[]{ scheme.text("") };
            return result;
        }
    }

    private static class CassandraStyleParameterRenderer implements IParameterRenderer
    {
        @Override
        public Ansi.Text[][] render(CommandLine.Model.PositionalParamSpec param, IParamLabelRenderer parameterLabelRenderer, ColorScheme scheme)
        {
            String descriptionString = param.description()[0];
            Ansi.Text descPadding = Ansi.OFF.new Text(leadingSpaces(DESCRIPTION_INDENT), scheme);
            Ansi.Text[][] result = new Ansi.Text[3][];
            result[0] = new Ansi.Text[]{ parameterLabelRenderer.renderParameterLabel(param, scheme.ansi(), scheme.parameterStyles()) };
            result[1] = new Ansi.Text[]{ descPadding.concat(scheme.parameterText(descriptionString)) };
            result[2] = new Ansi.Text[]{ Ansi.OFF.new Text("", scheme) };
            return result;
        }
    }

    private static class LineBreakingLayout
    {
        private static final int spaceWidth = 1;
        private final int width;
        private final TextTable textTable;
        private final Ansi.Text padding;
        /** Current line being built, always less than width. */
        private Ansi.Text current;

        public LineBreakingLayout(ColorScheme colorScheme, int width, TextTable textTable)
        {
            this.width = width - textTable.columns()[0].indent;
            this.padding = colorScheme.text(leadingSpaces(textTable.indentWrappedLines));
            this.textTable = textTable;
            current = colorScheme.text("");
        }

        public LineBreakingLayout concatItems(List<Ansi.Text> items)
        {
            for (Ansi.Text item : items)
                concatItem(item);
            return this;
        }

        public LineBreakingLayout concatItem(Ansi.Text item)
        {
            if (item.plainString().isEmpty())
                return this;

            if (current.plainString().length() + spaceWidth + item.plainString().length() >= width)
            {
                textTable.addRowValues(current);
                current = padding.concat(item);
            }
            else
                current = current == padding || current.plainString().isEmpty() ?
                          current.concat(item) :
                          current.concat(" ").concat(item);
            return this;
        }

        public void flush(Ansi.Text end)
        {
            textTable.addRowValues(current == padding ? end : current.concat(" ").concat(end));
        }
    }

    private static class OptionSpecByNamesComparator implements Comparator<CommandLine.Model.OptionSpec>
    {
        private final Comparator<CommandLine.Model.OptionSpec> comparator = createShortOptionNameComparator();
        @Override
        public int compare(CommandLine.Model.OptionSpec o1, CommandLine.Model.OptionSpec o2)
        {
            if (Objects.deepEquals(o1.names(), o2.names()))
                return 0;
            return comparator.compare(o1, o2);
        }
    }
}
