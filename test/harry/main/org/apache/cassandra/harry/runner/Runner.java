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

package org.apache.cassandra.harry.runner;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ExecutorFactory;
import org.apache.cassandra.concurrent.Interruptible;
import org.apache.cassandra.concurrent.Shutdownable;
import org.apache.cassandra.harry.core.Configuration;
import org.apache.cassandra.harry.core.Run;
import org.apache.cassandra.harry.sut.SystemUnderTest;
import org.apache.cassandra.harry.visitors.Visitor;
import org.apache.cassandra.utils.concurrent.WaitQueue;

import static org.apache.cassandra.concurrent.InfiniteLoopExecutor.Daemon.NON_DAEMON;
import static org.apache.cassandra.concurrent.InfiniteLoopExecutor.Interrupts.UNSYNCHRONIZED;
import static org.apache.cassandra.concurrent.InfiniteLoopExecutor.SimulatorSafe.SAFE;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

public abstract class Runner
{
    private static final Logger logger = LoggerFactory.getLogger(Runner.class);

    protected final Run run;
    protected final Configuration config;

    // If there's an error, there's a good chance we're going to hit it more than once
    // since we have multiple concurrent checkers running
    protected final List<Throwable> errors;

    public Runner(Run run, Configuration config)
    {
        this.run = run;
        this.config = config;
        this.errors = new CopyOnWriteArrayList<>();
    }

    public void run() throws Throwable
    {
        init(config, run);
        try
        {
            runInternal();
        }
        catch (EarlyExitException t)
        {
            logger.warn("Exiting early...", t);
        }
        teardown();
    }

    protected abstract void runInternal() throws Throwable;

    public Run getRun()
    {
        return run;
    }

    public static void init(Configuration config, Run run)
    {
        if (config.create_schema)
        {
            if (config.keyspace_ddl == null)
                run.sut.schemaChange("CREATE KEYSPACE IF NOT EXISTS " + run.schemaSpec.keyspace + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};");
            else
                run.sut.schemaChange(config.keyspace_ddl);

            String schema = run.schemaSpec.compile().cql();
            logger.info("Creating table: " + schema);
            run.sut.schemaChange(schema);
        }

        if (config.truncate_table)
        {
            run.sut.schemaChange(String.format("truncate %s.%s;",
                                               run.schemaSpec.keyspace,
                                               run.schemaSpec.table));
        }

        run.sut.afterSchemaInit();

        int res = run.sut.execute(String.format("SELECT * FROM %s.%s LIMIT 1", run.schemaSpec.keyspace, run.schemaSpec.table), SystemUnderTest.ConsistencyLevel.QUORUM).length;
        if (res > 0)
        {
            System.out.println("========================================================================================================================");
            System.out.println("|                                                                                                                      |");
            System.out.println("|                                   WARNING: Starting a run with non-empty tables!                                     |");
            System.out.println("|                                                                                                                      |");
            System.out.println("========================================================================================================================");
        }
    }

    public void teardown()
    {
        logger.info("Tearing down setup...");
        if (config.drop_schema)
        {
            if (!errors.isEmpty())
            {
                logger.info("Preserving table {} due to errors during execution.",
                            run.schemaSpec.table);
                return;
            }

            logger.info("Dropping table: " + run.schemaSpec.table);
            run.sut.schemaChange(String.format("DROP TABLE IF EXISTS %s.%s;",
                                               run.schemaSpec.keyspace,
                                               run.schemaSpec.table));
        }
    }

    protected void maybeReportErrors()
    {
        if (!errors.isEmpty()) {
            dumpStateToFile(run, config, errors);
        }
    }

    public abstract String type();
        
    public interface RunnerFactory
    {
        Runner make(Run run, Configuration config);
    }

    public abstract static class TimedRunner extends Runner
    {
        public final long runtime;
        public final TimeUnit runtimeUnit;

        public TimedRunner(Run run, Configuration config, long runtime, TimeUnit runtimeUnit)
        {
            super(run, config);

            this.runtime = runtime;
            this.runtimeUnit = runtimeUnit;
        }
    }

    public static Configuration.RunnerConfiguration single(Visitor.VisitorFactory visitor)
    {
        return (run, config) -> new SingleVisitRunner(run, config, Collections.singletonList(visitor));
    }

    public static class SingleVisitRunner extends Runner
    {
        private final List<Visitor> visitors;

        public SingleVisitRunner(Run run,
                                 Configuration config,
                                 List<? extends Visitor.VisitorFactory> visitorFactories)
        {
            super(run, config);
            this.visitors = visitorFactories.stream().map(factory -> factory.make(run)).collect(Collectors.toList());
        }

        @Override
        public String type()
        {
            return "single";
        }

        public void runInternal()
        {
            for (Visitor value : visitors)
                value.visit();
        }
    }

    public static Configuration.RunnerConfiguration sequential(Visitor.VisitorFactory visitor,
                                                               long runtime, TimeUnit runtimeUnit)
    {
        return (r, c) -> new SequentialRunner(r, c, Collections.singletonList(visitor), runtime, runtimeUnit);
    }

    public static Runner sequential(Run run,
                                    Configuration config,
                                    List<Visitor.VisitorFactory> poolConfigurations,
                                    long runtime, TimeUnit runtimeUnit)
    {
        return new SequentialRunner(run, config, poolConfigurations, runtime, runtimeUnit);
    }

    /**
     * Runs all visitors sequentially, in the loop, for a specified amount of time
     */
    public static class SequentialRunner extends TimedRunner
    {
        protected final List<Visitor> visitors;

        public SequentialRunner(Run run,
                                Configuration config,
                                List<? extends Visitor.VisitorFactory> visitorFactories,
                                long runtime, TimeUnit runtimeUnit)
        {
            super(run, config, runtime, runtimeUnit);

            this.visitors = visitorFactories.stream().map(factory -> factory.make(run)).collect(Collectors.toList());
        }

        @Override
        public String type()
        {
            return "sequential";
        }

        @Override
        public void runInternal() throws Throwable
        {
            long deadline = nanoTime() + runtimeUnit.toNanos(runtime);;
            while (nanoTime() < deadline)
            {
                for (Visitor visitor : visitors)
                    visitor.visit();
            }
        }
    }

    public static Runner concurrent(Configuration config,
                                    List<Configuration.VisitorPoolConfiguration> poolConfigurations,
                                    long runtime, TimeUnit runtimeUnit)
    {
        return new ConcurrentRunner(config.createRun(), config, poolConfigurations, runtime, runtimeUnit);
    }

    /**
     * Runs all visitors concurrently, each visitor in its own thread, looped, for a specified amount of time
     */
    public static class ConcurrentRunner extends TimedRunner
    {
        private final List<Configuration.VisitorPoolConfiguration> poolConfigurations;

        public ConcurrentRunner(Run run,
                                Configuration config,
                                List<Configuration.VisitorPoolConfiguration> poolConfigurations,
                                long runtime, TimeUnit runtimeUnit)
        {
            super(run, config, runtime, runtimeUnit);
            this.poolConfigurations = poolConfigurations;
        }

        @Override
        public String type()
        {
            return "concurrent";
        }

        public void runInternal() throws Throwable
        {
            List<Interruptible> threads = new ArrayList<>();
            WaitQueue queue = WaitQueue.newWaitQueue();
            WaitQueue.Signal interrupt = queue.register();

            for (Configuration.VisitorPoolConfiguration poolConfiguration : poolConfigurations)
            {
                for (int i = 0; i < poolConfiguration.concurrency; i++)
                {
                    Visitor visitor = poolConfiguration.visitor.make(run);
                    String name = String.format("%s-%d", poolConfiguration.prefix, i + 1);
                    Interruptible thread = ExecutorFactory.Global.executorFactory().infiniteLoop(name, wrapInterrupt((state) -> {
                        if (state == Interruptible.State.NORMAL)
                            visitor.visit();
                    }, interrupt::signal, errors::add), SAFE, NON_DAEMON, UNSYNCHRONIZED);
                    threads.add(thread);
                }
            }

            interrupt.await(runtime, runtimeUnit);
            shutdown(threads::stream);
            if (!errors.isEmpty())
                mergeAndThrow(errors);
        }
    }

    public static Runner chain(Configuration config,
                               Configuration.RunnerConfiguration... runners)
    {
        return new ChainRunner(config.createRun(), config, Arrays.asList(runners));
    }

    /**
     * Chain runner is somewhat similar to StagedRunner in that it would execute multiple runners one after
     * another, but unlike StagedRunner, it is single-shot: it will just run them once.
     */
    public static class ChainRunner extends Runner
    {
        private final List<Runner> stages;

        public ChainRunner(Run run,
                           Configuration config,
                           List<Configuration.RunnerConfiguration> runnerFactories)
        {
            super(run, config);
            this.stages = new ArrayList<>();
            for (Configuration.RunnerConfiguration runner : runnerFactories)
                stages.add(runner.make(run, config));
        }

        protected void runInternal() throws Throwable
        {
            for (Runner stage : stages)
                stage.runInternal();
        }

        public String type()
        {
            return "chain";
        }
    }

    public static void shutdown(Supplier<Stream<Interruptible>> threads)
    {
        threads.get().forEach(Shutdownable::shutdown);
        long deadline = nanoTime();
        threads.get().forEach((interruptible) -> {
            try
            {
                if (!interruptible.awaitTermination(nanoTime() - deadline, TimeUnit.NANOSECONDS))
                    logger.info("Could not terminate before the timeout: " + threads.get().map(Shutdownable::isTerminated).collect(Collectors.toList()));
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
        });

    }

    public static Interruptible.Task wrapInterrupt(Interruptible.Task runnable, Runnable signalStop, Consumer<Throwable> registerException)
    {
        return (state) -> {
            try
            {
                runnable.run(state);
            }
            catch (InterruptedException t)
            {
                signalStop.run();
            }
            catch (Throwable t)
            {
                // Since some of the exceptions are thrown from inside instances
                if (!t.getClass().toString().contains("InterruptedException"))
                    registerException.accept(t);
                signalStop.run();
            }
        };
    }

    public static void mergeAndThrow(List<Throwable> existingFail)
    {
        List<Throwable> skipped = existingFail.stream().filter(e -> e instanceof EarlyExitException).collect(Collectors.toList());
        for (Throwable throwable : skipped)
        {
            logger.warn("Skipping exit early exceptions", throwable);
            return;
        }

        List<Throwable> errors = existingFail.stream().filter(e -> !(e instanceof EarlyExitException)).collect(Collectors.toList());
        if (errors.size() == 1)
            throw new RuntimeException("Interrupting run because of an exception", errors.get(0));

        Throwable e = errors.get(0);
        Throwable ret = e;
        for (int i = 1; i < errors.size(); i++)
        {
            Throwable current = errors.get(i);
            e.addSuppressed(current);
            e = current;
        }

        throw new RuntimeException("Interrupting run because of an exception", ret);
    }

    private static void dumpExceptionToFile(BufferedWriter bw, Throwable throwable) throws IOException
    {
        if (throwable.getMessage() != null)
            bw.write(throwable.getMessage());
        else
            bw.write("<no message>");

        bw.newLine();
        for (StackTraceElement line : throwable.getStackTrace())
        {
            bw.write(line.toString());
            bw.newLine();
        }
        bw.newLine();
        if (throwable.getCause() != null)
        {
            bw.write("Inner Exception: ");
            dumpExceptionToFile(bw, throwable.getCause());
        }

        bw.newLine();
        bw.newLine();
    }

    public static void dumpStateToFile(Run run, Configuration config, List<Throwable> t)
    {
        try
        {
            File f = new File("failure.dump");
            logger.error("Dumping results into the file:" + f);
            try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(f))))
            {
                bw.write("Caught exception during the run: ");
                for (Throwable throwable : t)
                    dumpExceptionToFile(bw, throwable);

                bw.flush();
            }

            File file = new File("run.yaml");
            Configuration.ConfigurationBuilder builder = config.unbuild();

            // override stateful components
            builder.setClock(run.clock.toConfig());
            builder.setDataTracker(run.tracker.toConfig());

            Configuration.toFile(file, builder.build());
        }

        catch (Throwable e)
        {
            logger.error("Caught an error while trying to dump to file", e);
            try
            {
                File f = new File("tmp.dump");
                
                if (!f.createNewFile())
                    logger.info("File {} already exists. Appending...", f);
                
                BufferedWriter tmp = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(f)));
                dumpExceptionToFile(tmp, e);
            }
            catch (IOException ex)
            {
                ex.printStackTrace();
            }

            throw new RuntimeException(e);
        }
    }
}