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

import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
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
import javax.management.MBeanServerConnection;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterDescription;
import com.beust.jcommander.Parameterized;
import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.tools.Output;
import org.gridkit.jvmtool.JmxConnectionInfo;
import org.gridkit.jvmtool.cli.CommandLauncher;

import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

@Command(name = "sjk", description = "Run commands of 'Swiss Java Knife'. Run 'nodetool sjk --help' for more information.")
public class Sjk extends NodeToolCmd
{
    @Arguments(description = "Arguments passed as is to 'Swiss Java Knife'.")
    private List<String> args;

    private final Wrapper wrapper = new Wrapper();

    @Override
    public void runInternal()
    {
        wrapper.prepare(args != null ? args.toArray(new String[0]) : new String[]{"help"}, output.out, output.err);

        if (!wrapper.requiresMbeanServerConn())
        {
            // SJK command does not require an MBeanServerConnection, so just invoke it
            wrapper.run(null, output);
        }
        else
        {
            // invoke common nodetool handling to establish MBeanServerConnection
            super.runInternal();
        }
    }

    public void sequenceRun(NodeProbe probe)
    {
        wrapper.prepare(args != null ? args.toArray(new String[0]) : new String[]{"help"}, probe.output().out, probe.output().err);
        if (!wrapper.run(probe, probe.output()))
            probe.failed();
    }

    protected void execute(NodeProbe probe)
    {
        if (!wrapper.run(probe, probe.output()))
            probe.failed();
    }

    /**
     * Adopted copy of {@link org.gridkit.jvmtool.SJK} from <a href="https://github.com/aragozin/jvm-tools">https://github.com/aragozin/jvm-tools</a>.
     */
    public static class Wrapper extends CommandLauncher
    {
        boolean suppressSystemExit;

        private final Map<String, Runnable> commands = new HashMap<>();

        private JCommander parser;

        private Runnable cmd;

        public void suppressSystemExit()
        {
            suppressSystemExit = true;
            super.suppressSystemExit();
        }

        public boolean start(String[] args)
        {
            throw new UnsupportedOperationException();
        }

        public void prepare(String[] args, PrintStream out, PrintStream err)
        {
            try
            {

                parser = new JCommander(this);

                addCommands();

                fixCommands();

                try
                {
                    parser.parse(args);
                }
                catch (Exception e)
                {
                    failAndPrintUsage(e.toString());
                }

                if (isHelp())
                {
                    String cmd = parser.getParsedCommand();
                    if (cmd == null)
                    {
                        parser.usage();
                    }
                    else
                    {
                        parser.usage(cmd);
                    }
                }
                else if (isListCommands())
                {
                    for (String cmd : commands.keySet())
                    {
                        out.println(String.format("%8s - %s", cmd, parser.getCommandDescription(cmd)));
                    }
                }
                else
                {

                    cmd = commands.get(parser.getParsedCommand());

                    if (cmd == null)
                    {
                        failAndPrintUsage();
                    }
                }
            }
            catch (CommandAbortedError error)
            {
                for (String m : error.messages)
                {
                    err.println(m);
                }
                if (isVerbose() && error.getCause() != null)
                {
                    error.getCause().printStackTrace(err);
                }
                if (error.printUsage && parser != null)
                {
                    printUsage(parser, out, parser.getParsedCommand());
                }
            }
            catch (Throwable e)
            {
                e.printStackTrace(err);
            }
        }

        void printUsage(JCommander parser, PrintStream out, String optionalCommand)
        {
            StringBuilder sb = new StringBuilder();
            if (optionalCommand != null)
                parser.usage(sb, optionalCommand);
            else
                parser.usage(sb);
            out.println(sb.toString());
        }

        public boolean run(final NodeProbe probe, final Output output)
        {
            PrintStream out = output.out;
            PrintStream err = output.err;
            try
            {
                setJmxConnInfo(probe);

                if (cmd != null)
                    cmd.run();

                return true;
            }
            catch (CommandAbortedError error)
            {
                for (String m : error.messages)
                {
                    err.println(m);
                }
                if (isVerbose() && error.getCause() != null)
                {
                    error.getCause().printStackTrace(err);
                }
                if (error.printUsage && parser != null)
                {
                    printUsage(parser, out, parser.getParsedCommand());
                }
                return true;
            }
            catch (Throwable e)
            {
                e.printStackTrace(err);
            }

            // abnormal termination
            return false;
        }

        private void setJmxConnInfo(final NodeProbe probe) throws IllegalAccessException
        {
            Field f = jmxConnectionInfoField(cmd);
            if (f != null)
            {
                f.setAccessible(true);
                f.set(cmd, new JmxConnectionInfo(this)
                {
                    public MBeanServerConnection getMServer()
                    {
                        return probe.getMbeanServerConn();
                    }
                });
            }
            f = pidField(cmd);
            if (f != null)
            {
                long pid = probe.getPid();

                f.setAccessible(true);
                if (f.getType() == int.class)
                    f.setInt(cmd, (int) pid);
                if (f.getType() == long.class)
                    f.setLong(cmd, pid);
                if (f.getType() == String.class)
                    f.set(cmd, Long.toString(pid));
            }
        }

        private boolean isHelp()
        {
            try
            {
                Field f = CommandLauncher.class.getDeclaredField("help");
                f.setAccessible(true);
                return f.getBoolean(this);
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }

        private boolean isListCommands()
        {
            try
            {
                Field f = CommandLauncher.class.getDeclaredField("listCommands");
                f.setAccessible(true);
                return f.getBoolean(this);
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }

        protected List<String> getCommandPackages()
        {
            return Collections.singletonList("org.gridkit.jvmtool.cmd");
        }

        private void addCommands() throws InstantiationException, IllegalAccessException
        {
            for (String pack : getCommandPackages())
            {
                for (Class<?> c : findClasses(pack))
                {
                    if (CommandLauncher.CmdRef.class.isAssignableFrom(c))
                    {
                        CommandLauncher.CmdRef cmd = (CommandLauncher.CmdRef) c.newInstance();
                        String cmdName = cmd.getCommandName();
                        Runnable cmdTask = cmd.newCommand(this);
                        if (commands.containsKey(cmdName))
                        {
                            fail("Ambiguous implementation for '" + cmdName + '\'');
                        }
                        commands.put(cmdName, cmdTask);
                        parser.addCommand(cmdName, cmdTask);
                    }
                }
            }
        }

        private void fixCommands() throws Exception
        {
            List<Field> fields = Arrays.asList(JCommander.class.getDeclaredField("m_fields"),
                                               JCommander.class.getDeclaredField("m_requiredFields"));
            for (Field f : fields)
                f.setAccessible(true);

            for (JCommander cmdr : parser.getCommands().values())
            {
                for (Field field : fields) {
                    Map<Parameterized, ParameterDescription> fieldsMap = (Map<Parameterized, ParameterDescription>) field.get(cmdr);
                    for (Iterator<Map.Entry<Parameterized, ParameterDescription>> iPar = fieldsMap.entrySet().iterator(); iPar.hasNext(); )
                    {
                        Map.Entry<Parameterized, ParameterDescription> par = iPar.next();
                        switch (par.getKey().getName())
                        {
                            // JmxConnectionInfo fields
                            case "pid":
                            case "sockAddr":
                            case "user":
                            case "password":
                                //
                            case "verbose":
                            case "help":
                            case "listCommands":
                                iPar.remove();
                                break;
                        }
                    }
                }
            }
        }

        boolean requiresMbeanServerConn()
        {
            return jmxConnectionInfoField(cmd) != null || pidField(cmd) != null;
        }

        private static Field jmxConnectionInfoField(Runnable cmd)
        {
            if (cmd == null)
                return null;

            for (Field f : cmd.getClass().getDeclaredFields())
            {
                if (f.getType() == JmxConnectionInfo.class)
                {
                    return f;
                }
            }
            return null;
        }

        private static Field pidField(Runnable cmd)
        {
            if (cmd == null)
                return null;

            for (Field f : cmd.getClass().getDeclaredFields())
            {
                if ("pid".equals(f.getName()) &&
                    (f.getType() == int.class || f.getType() == long.class || f.getType() == String.class))
                {
                    return f;
                }
            }
            return null;
        }

        private static List<Class<?>> findClasses(String packageName)
        {
            // TODO this will probably fail with JPMS/Jigsaw

            List<Class<?>> result = new ArrayList<>();
            try
            {
                String path = packageName.replace('.', '/');
                for (String f : findFiles(path))
                {
                    if (f.endsWith(".class") && f.indexOf('$') < 0)
                    {
                        f = f.substring(0, f.length() - ".class".length());
                        f = f.replace('/', '.');
                        result.add(Class.forName(f));
                    }
                }
                return result;
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }

        static List<String> findFiles(String path) throws IOException
        {
            List<String> result = new ArrayList<>();
            ClassLoader cl = Thread.currentThread().getContextClassLoader();
            Enumeration<URL> en = cl.getResources(path);
            while (en.hasMoreElements())
            {
                URL u = en.nextElement();
                listFiles(result, u, path);
            }
            return result;
        }

        static void listFiles(List<String> results, URL packageURL, String path) throws IOException
        {
            if (packageURL.getProtocol().equals("jar"))
            {
                String jarFileName;
                Enumeration<JarEntry> jarEntries;
                String entryName;

                // build jar file name, then loop through zipped entries
                jarFileName = URLDecoder.decode(packageURL.getFile(), "UTF-8");
                jarFileName = jarFileName.substring(5, jarFileName.indexOf('!'));
                try (JarFile jf = new JarFile(jarFileName))
                {
                    jarEntries = jf.entries();
                    while (jarEntries.hasMoreElements())
                    {
                        entryName = jarEntries.nextElement().getName();
                        if (entryName.startsWith(path))
                        {
                            results.add(entryName);
                        }
                    }
                }
            }
            else
            {
                // loop through files in classpath
                File dir = new File(packageURL.getFile());
                String cp = dir.canonicalPath();
                File root = dir;
                while (true)
                {
                    if (cp.equals(new File(root, path).canonicalPath()))
                    {
                        break;
                    }
                    root = root.parent();
                }
                listFiles(results, root, dir);
            }
        }

        static void listFiles(List<String> names, File root, File dir)
        {
            String rootPath = root.absolutePath();
            if (dir.exists() && dir.isDirectory())
            {
                for (File file : dir.tryList())
                {
                    if (file.isDirectory())
                    {
                        listFiles(names, root, file);
                    }
                    else
                    {
                        String name = file.absolutePath().substring(rootPath.length() + 1);
                        name = name.replace('\\', '/');
                        names.add(name);
                    }
                }
            }
        }
    }
}
