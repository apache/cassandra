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

import java.io.PrintWriter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import org.apache.cassandra.tools.nodetool.layout.CassandraCliHelpLayout;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.IHelpCommandInitializable2;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import static picocli.CommandLine.Model.UsageMessageSpec.SECTION_KEY_COMMAND_LIST;
import static picocli.CommandLine.Model.UsageMessageSpec.SECTION_KEY_COMMAND_LIST_HEADING;
import static picocli.CommandLine.Model.UsageMessageSpec.SECTION_KEY_EXIT_CODE_LIST;
import static picocli.CommandLine.Model.UsageMessageSpec.SECTION_KEY_EXIT_CODE_LIST_HEADING;
import static picocli.CommandLine.Model.UsageMessageSpec.SECTION_KEY_FOOTER;
import static picocli.CommandLine.Model.UsageMessageSpec.SECTION_KEY_FOOTER_HEADING;
import static picocli.CommandLine.Model.UsageMessageSpec.SECTION_KEY_HEADER;
import static picocli.CommandLine.Model.UsageMessageSpec.SECTION_KEY_HEADER_HEADING;
import static picocli.CommandLine.Model.UsageMessageSpec.SECTION_KEY_SYNOPSIS;

@Command(name = "help",
         helpCommand = true,
         description = "Display help information")
public class Help implements IHelpCommandInitializable2, Runnable
{
    @Option(names = { "--help" }, hidden = true, usageHelp = true, descriptionKey = "helpCommand.help",
            description = "Show usage help for the help command and exit.")
    private boolean helpRequested;

    @Parameters(paramLabel = "command", arity = "1..*", descriptionKey = "helpCommand.command",
                description = "The COMMAND to display the usage help message for.")
    private List<String> commands;

    private CommandLine self;
    private PrintWriter out;
    private CommandLine.Help.ColorScheme colorScheme;

    /**
     * Invokes {@code #usage(PrintStream, CommandLine.Help.ColorScheme) usage} for the specified command,
     * or for the parent command.
     */
    @Override
    public void run()
    {
        CommandLine parent = self == null ? null : self.getParent();
        if (parent == null)
            return;

        CommandLine.Help.ColorScheme colors = colorScheme == null ?
                                              CommandLine.Help.defaultColorScheme(CommandLine.Help.Ansi.AUTO) :
                                              colorScheme;

        if (commands == null)
        {
            // If the parent command is the top-level command, print help for the top-level command.
            printTopCommandUsage(parent, colors, out);
            return;
        }

        if (parent.isAbbreviatedSubcommandsAllowed())
            throw new CommandLine.ParameterException(parent, "Abbreviated subcommands are not allowed.");

        // Print help for the last command in the list of commands.
        CommandLine subcommand = parent;
        for (String command : commands)
        {
            subcommand = subcommand.getSubcommands().get(command);
            if (subcommand == null)
                throw new CommandLine.ParameterException(parent, "Unknown subcommand '" + command + "'.", null, command);
        }

        subcommand.usage(out, colors);
    }

    public static void printTopCommandUsage(CommandLine command, CommandLine.Help.ColorScheme colors, PrintWriter writer)
    {
        if (command == null)
            return;

        StringBuilder sb = new StringBuilder();
        CommandLine.Help help = command.getHelpFactory().create(command.getCommandSpec(), colors);
        if (!(help instanceof CassandraCliHelpLayout))
        {
            command.usage(writer, colors);
            return;
        }

        Map<String, CommandLine.IHelpSectionRenderer> helpSectionMap = cassandraTopLevelHelpSectionKeys((CassandraCliHelpLayout) help);
        for (String key : command.getHelpSectionKeys())
        {
            CommandLine.IHelpSectionRenderer renderer = helpSectionMap.get(key);
            if (renderer == null)
                continue;
            String rendered = renderer.render(help);
            if (!Strings.isNullOrEmpty(rendered))
                sb.append(rendered);
        }

        writer.println(sb);
        writer.flush();
    }

    /**
     * Top-level help command (includes all the available nodetool commands) has a different layout, so we need to
     * provide a different set of keys for the help sections.
     * @param layout The help class layout.
     * @return Map of supported keys for the help sections.
     */
    public static Map<String, CommandLine.IHelpSectionRenderer> cassandraTopLevelHelpSectionKeys(CassandraCliHelpLayout layout)
    {
        Map<String, CommandLine.IHelpSectionRenderer> sectionMap = new LinkedHashMap<>();
        sectionMap.put(SECTION_KEY_HEADER_HEADING, CommandLine.Help::headerHeading);
        sectionMap.put(SECTION_KEY_HEADER, CommandLine.Help::header);
        sectionMap.put(SECTION_KEY_SYNOPSIS, layout::topLevelSynopsis);
        sectionMap.put(SECTION_KEY_COMMAND_LIST_HEADING, layout::topLevelCommandListHeading);
        sectionMap.put(SECTION_KEY_COMMAND_LIST, layout::topLevelCommandList);
        sectionMap.put(SECTION_KEY_EXIT_CODE_LIST_HEADING, CommandLine.Help::exitCodeListHeading);
        sectionMap.put(SECTION_KEY_EXIT_CODE_LIST, CommandLine.Help::exitCodeList);
        sectionMap.put(SECTION_KEY_FOOTER_HEADING, CommandLine.Help::footerHeading);
        sectionMap.put(SECTION_KEY_FOOTER, CommandLine.Help::footer);
        return sectionMap;
    }

    /**
     * The printHelpIfRequested method calls the init method on commands marked
     * as helpCommand before the help command's run or call method is called.
     */
    public void init(CommandLine helpCommandLine,
                     CommandLine.Help.ColorScheme colorScheme,
                     PrintWriter out,
                     PrintWriter err)
    {
        this.self = Preconditions.checkNotNull(helpCommandLine, "helpCommandLine");
        this.colorScheme = Preconditions.checkNotNull(colorScheme, "colorScheme");
        this.out = Preconditions.checkNotNull(out, "outWriter");
    }
}
