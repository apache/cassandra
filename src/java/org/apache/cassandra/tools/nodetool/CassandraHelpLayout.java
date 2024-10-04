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

package org.apache.cassandra.tools.nodetool;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.utils.Pair;
import picocli.CommandLine;

import static org.apache.cassandra.tools.nodetool.CommandUtils.findBackwardCompatibleArgument;
import static org.apache.cassandra.tools.nodetool.CommandUtils.leadingSpaces;
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
 * Help factory for the Cassandra nodetool to generate the help output. This class is used to match
 * the command output with the previously available nodetool help output format.
 */
public class CassandraHelpLayout extends CommandLine.Help
{
    public static final int DEFAULT_USAGE_HELP_WIDTH = 85;
    private static final String DESCRIPTION_HEADING = "NAME%n";
    private static final String SYNOPSIS_HEADING = "SYNOPSIS%n";
    private static final String OPTIONS_HEADING = "OPTIONS%n";
    private static final String FOOTER_HEADING = "%n";
    private static final int DESCRIPTION_INDENT = 4;
    public static final int COLUMN_INDENT = 8;
    public static final int SUBCOMMANDS_INDENT = 4;
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
    private static final String[] EMPTY_FOOTER = new String[0];

    public CassandraHelpLayout(CommandLine.Model.CommandSpec spec, ColorScheme scheme)
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
    public String description(Object... params) {
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

    @Override
    public String synopsis(int synopsisHeadingLength)
    {
        return printDetailedSynopsis("", COLUMN_INDENT, true);
    }

    private Ansi.Text createCassandraSynopsisCommandText()
    {
        Ansi.Text commandText = ansi().new Text(0);
        if (!commandSpec().subcommands().isEmpty())
            return commandText.concat(SYNOPSIS_SUBCOMMANDS_LABEL);
        return commandText;
    }

    private String printDetailedSynopsis(String synopsisPrefix, int columnIndent, boolean showEndOfOptionsDelimiter)
    {
        // Cassandra uses end of options delimiter in usage help.
        commandSpec().usageMessage().showEndOfOptionsDelimiterInUsageHelp(showEndOfOptionsDelimiter);

        CommandLine.Model.CommandSpec commandSpec = commandSpec();
        ColorScheme colorScheme = colorScheme();

        List<Ansi.Text> parentOptionsList = createCassandraSynopsisOptionsText(parentCommandOptions(commandSpec));
        List<Ansi.Text> commandOptionsList = createCassandraSynopsisOptionsText(commandSpec.options());

        Ansi.Text positionalParamText = createCassandraSynopsisPositionalsText(commandSpec, colorScheme);
        Ansi.Text endOfOptionsText = positionalParamText.plainString().isEmpty() ?
                                     colorScheme.text("") :
                                     colorScheme.text("[")
                                                .concat(commandSpec.parser().endOfOptionsDelimiter())
                                                .concat("]");
        Ansi.Text commandText = createCassandraSynopsisCommandText();

        int width = commandSpec.usageMessage().width();
        boolean isEmptyParent = commandSpec.parent() == null;
        Ansi.Text mainCommandText = isEmptyParent ? colorScheme.commandText(commandSpec.name()) :
                                    colorScheme.commandText(commandSpec.parent().qualifiedName());
        TextTable textTable = TextTable.forColumns(colorScheme, new Column(width, columnIndent, Column.Overflow.WRAP));
        textTable.indentWrappedLines = COLUMN_INDENT;
        textTable.setAdjustLineBreaksForWideCJKCharacters(commandSpec.usageMessage().adjustLineBreaksForWideCJKCharacters());

        // List<Text>
        new LineBreakingLayout(colorScheme, width, textTable)
            .concatItem(synopsisPrefix.isEmpty() ? mainCommandText : colorScheme.text(synopsisPrefix).concat(" ").concat(mainCommandText))
            .concatItems(parentOptionsList)
            .concatItem(isEmptyParent ? colorScheme.text("") : colorScheme.text(commandSpec.name()))
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

        Pair<String, String> commandArgumensSpec = findBackwardCompatibleArgument(spec.userObject());
        Ansi.Text text = colorScheme.text("");
        // If the command has a backward compatible argument, use it to generate the synopsis based on the old format.
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

    private static List<CommandLine.Model.OptionSpec> parentCommandOptions(CommandLine.Model.CommandSpec commandSpec)
    {
        List<CommandLine.Model.CommandSpec> hierarhy = new LinkedList<>();
        CommandLine.Model.CommandSpec curr;
        while ((curr = commandSpec.parent()) != null)
        {
            hierarhy.add(curr);
            commandSpec = curr;
        }
        Collections.reverse(hierarhy);
        List<CommandLine.Model.OptionSpec> options = new ArrayList<>();
        for (CommandLine.Model.CommandSpec spec : hierarhy)
            options.addAll(spec.options());
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
                text = text.concat("[(")
                           .concat(shortName)
                           .concat(spacedParamLabel(option, parameterLabelRenderer, colorScheme))
                           .concat(" | ")
                           .concat(fullName)
                           .concat(spacedParamLabel(option, parameterLabelRenderer, colorScheme))
                           .concat(")]");
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

    @Override
    public String optionList()
    {
        Comparator<CommandLine.Model.OptionSpec> comparator = createShortOptionNameComparator();
        List<CommandLine.Model.OptionSpec> options = new LinkedList<>(parentCommandOptions(commandSpec()));
        options.addAll(commandSpec().options());
        options.sort(comparator);

        Layout layout = cassandraSingleColumnOptionsParametersLayout();
        layout.addAllOptions(options, CassandraStyleParamLabelRender.create());
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
        Pair<String, String> cassandraArgument = findBackwardCompatibleArgument(commandSpec().userObject());
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
    public String commandList(Map<String, CommandLine.Help> subcommands)
    {
        if (subcommands.isEmpty())
            return "";
        int width = commandSpec().usageMessage().width();
        int commandLength = Math.min(CommandUtils.maxLength(subcommands.keySet()), width / 2);
        int leadinColumnWidth = commandLength + SUBCOMMANDS_INDENT;
        TextTable table = TextTable.forColumns(colorScheme(),
                                                                new Column(leadinColumnWidth, SUBCOMMANDS_INDENT,
                                                                                            Column.Overflow.SPAN),
                                                                new Column(width - leadinColumnWidth, SUBCOMMANDS_INDENT,
                                                                                            Column.Overflow.WRAP));
        table.setAdjustLineBreaksForWideCJKCharacters(commandSpec().usageMessage().adjustLineBreaksForWideCJKCharacters());

        for (Map.Entry<String, CommandLine.Help> entry : subcommands.entrySet())
        {
            CommandLine.Help help = entry.getValue();
            CommandLine.Model.UsageMessageSpec usage = help.commandSpec().usageMessage();
            String header = isEmpty(usage.header()) ? (isEmpty(usage.description()) ? "" : usage.description()[0]) : usage.header()[0];
            Ansi.Text[] lines = colorScheme().text(header).splitLines();
            for (int i = 0; i < lines.length; i++)
                table.addRowValues(i == 0 ? help.commandNamesText(", ") : Ansi.OFF.new Text(0), lines[i]);
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

    public String topLevelSynopsis(Object... params)
    {
        return printDetailedSynopsis(TOP_LEVEL_SYNOPSIS_LIST_PREFIX, 0, false);
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
                if (field.getName().equals(argSpec.paramLabel()))
                    return argSpec.paramLabel();
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
            Ansi.Text desc = scheme.optionText(option.description().length == 0 ? "" : option.description()[0]);

            Ansi.Text[][] result = new Ansi.Text[3][];
            result[0] = new Ansi.Text[]{ optionText };
            result[1] = new Ansi.Text[]{ descPadding.concat(desc) };
            result[2] = new Ansi.Text[]{ Ansi.OFF.new Text("", scheme) };
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
}
