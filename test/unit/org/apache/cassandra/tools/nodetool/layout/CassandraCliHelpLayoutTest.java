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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.tools.nodetool.Help;

import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import static org.apache.cassandra.tools.nodetool.layout.CassandraCliHelpLayout.USAGE_HELP_FOOTER;
import static org.assertj.core.api.Assertions.assertThat;

public class CassandraCliHelpLayoutTest
{
    @Test
    public void testSubcommandFooterIsShown()
    {
        String usage = usage("withfooter");
        assertThat(usage).endsWith(String.format("A flag%n%nEXAMPLES%n        root withfooter --flag"));
        assertThat(usage).doesNotContain(USAGE_HELP_FOOTER);
    }

    @Test
    public void testSubcommandWithoutFooterIsUnchanged()
    {
        String usage = usage("withoutfooter");
        assertThat(usage).endsWith("A flag");
        assertThat(usage).doesNotContain(USAGE_HELP_FOOTER);
    }

    @Test
    public void testRootFooterIsUnchanged()
    {
        StringWriter out = new StringWriter();
        Help.printTopCommandUsage(commandLine(), CommandLine.Help.defaultColorScheme(CommandLine.Help.Ansi.OFF), new PrintWriter(out));
        assertThat(out.toString().trim()).endsWith(USAGE_HELP_FOOTER);
        assertThat(out.toString()).doesNotContain("EXAMPLES");
    }

    @Test
    public void testOptionalPositionalsAreBracketedInSynopsis()
    {
        String usage = usage("withargs");
        assertThat(usage).contains("withargs [--] [<keyspace>] [<tables>...]");
        // The arguments section keeps the plain labels.
        assertThat(usage).contains(String.format("        <keyspace>%n            The keyspace"));
        assertThat(usage).contains(String.format("        <tables>%n            The tables"));
    }

    @Test
    public void testRequiredOptionsAreNotBracketed()
    {
        String usage = usage("withoptions");
        assertThat(usage).contains("withoptions [--epoch <epoch>] --name <name>");
        assertThat(usage).doesNotContain("[--name <name>]");
    }

    @Test
    public void testAllParameterDescriptionLinesAreShown()
    {
        String usage = usage("withargs");
        assertThat(usage).contains(String.format("            The tables%n            <table> [<table>...]"));
    }

    private static String usage(String subcommand)
    {
        return commandLine().getSubcommands().get(subcommand).getUsageMessage(CommandLine.Help.Ansi.OFF).trim();
    }

    private static CommandLine commandLine()
    {
        return new CommandLine(new Root()).setHelpFactory(CassandraCliHelpLayout::new)
                                          .setUsageHelpWidth(CassandraCliHelpLayout.DEFAULT_USAGE_HELP_WIDTH)
                                          .setHelpSectionKeys(CassandraCliHelpLayout.cassandraHelpSectionKeys());
    }

    @Command(name = "root", subcommands = { WithFooter.class, WithoutFooter.class, WithArgs.class, WithOptions.class })
    public static class Root implements Runnable
    {
        public void run() {}
    }

    @Command(name = "withfooter", description = "Command with a footer", footer = { "EXAMPLES", "        root withfooter --flag" })
    public static class WithFooter implements Runnable
    {
        @Option(names = "--flag", description = "A flag")
        private boolean flag;

        public void run() {}
    }

    @Command(name = "withoutfooter", description = "Command without a footer")
    public static class WithoutFooter implements Runnable
    {
        @Option(names = "--flag", description = "A flag")
        private boolean flag;

        public void run() {}
    }

    @Command(name = "withargs", description = "Command with optional arguments")
    public static class WithArgs implements Runnable
    {
        @Parameters(index = "0", arity = "0..1", paramLabel = "keyspace", description = "The keyspace")
        private String keyspace;

        @Parameters(index = "1..*", paramLabel = "tables", description = { "The tables", "<table> [<table>...]" })
        private List<String> tables;

        public void run() {}
    }

    @Command(name = "withoptions", description = "Command with required options")
    public static class WithOptions implements Runnable
    {
        @Option(names = "--name", paramLabel = "name", required = true, description = "A required option")
        private String name;

        @ArgGroup(exclusive = false, multiplicity = "0..1")
        private Group group;

        public void run() {}
    }

    public static class Group
    {
        @Option(names = "--epoch", paramLabel = "epoch", required = true, description = "Required together with the group")
        private String epoch;
    }
}
