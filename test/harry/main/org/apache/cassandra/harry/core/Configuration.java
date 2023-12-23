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

package org.apache.cassandra.harry.core;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.annotation.Annotation;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import org.apache.cassandra.harry.ddl.SchemaGenerators;
import org.apache.cassandra.harry.ddl.SchemaSpec;
import org.apache.cassandra.harry.gen.Surjections;
import org.apache.cassandra.harry.gen.distribution.Distribution;
import org.apache.cassandra.harry.model.Model;
import org.apache.cassandra.harry.model.OpSelectors;
import org.apache.cassandra.harry.model.NoOpChecker;
import org.apache.cassandra.harry.model.QuiescentChecker;
import org.apache.cassandra.harry.clock.ApproximateClock;
import org.apache.cassandra.harry.sut.SystemUnderTest;
import org.apache.cassandra.harry.tracker.LockingDataTracker;
import org.apache.cassandra.harry.tracker.DataTracker;
import org.apache.cassandra.harry.tracker.DefaultDataTracker;
import org.apache.cassandra.harry.runner.Runner;
import org.apache.cassandra.harry.util.BitSet;
import org.apache.cassandra.harry.visitors.AllPartitionsValidator;
import org.apache.cassandra.harry.visitors.CorruptingVisitor;
import org.apache.cassandra.harry.visitors.LoggingVisitor;
import org.apache.cassandra.harry.visitors.MutatingRowVisitor;
import org.apache.cassandra.harry.visitors.MutatingVisitor;
import org.apache.cassandra.harry.visitors.OperationExecutor;
import org.apache.cassandra.harry.visitors.QueryLogger;
import org.apache.cassandra.harry.visitors.RandomValidator;
import org.apache.cassandra.harry.visitors.RecentValidator;
import org.apache.cassandra.harry.visitors.Visitor;
import org.reflections.Reflections;
import org.reflections.scanners.Scanners;
import org.reflections.util.NameHelper;

public class Configuration
{
    private enum NameUtils implements NameHelper
    {
        INSTANCE;
    }

    private static final ObjectMapper mapper;

    private static <A extends Annotation> Set<Class<?>> findClassesMarkedWith(Class<A> annotation)
    {
        Reflections reflections = new Reflections(org.reflections.util.ConfigurationBuilder.build("harry").setExpandSuperTypes(false));
        Collection<Class<?>> classes = NameUtils.INSTANCE.forNames(reflections.get(Scanners.TypesAnnotated.get(annotation.getName())),
                                                                   reflections.getConfiguration().getClassLoaders());
        return new HashSet<>(classes);
    }

    static
    {
        mapper = new ObjectMapper(new YAMLFactory() // checkstyle: permit this instantiation
                                  .disable(YAMLGenerator.Feature.USE_NATIVE_TYPE_ID)
                                  .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
                                  .disable(YAMLGenerator.Feature.CANONICAL_OUTPUT)
                                  .enable(YAMLGenerator.Feature.INDENT_ARRAYS));

        findClassesMarkedWith(JsonTypeName.class)
        .forEach(mapper::registerSubtypes);
    }

    public final long seed;
    public final SchemaProviderConfiguration schema_provider;

    public final boolean drop_schema;
    public final String keyspace_ddl;
    public final boolean create_schema;
    public final boolean truncate_table;

    public final MetricReporterConfiguration metric_reporter;
    public final ClockConfiguration clock;
    public final SutConfiguration system_under_test;
    public final DataTrackerConfiguration data_tracker;
    public final RunnerConfiguration runner;
    public final PDSelectorConfiguration partition_descriptor_selector;
    public final CDSelectorConfiguration clustering_descriptor_selector;

    @JsonCreator
    public Configuration(@JsonProperty("seed") long seed,
                         @JsonProperty("schema_provider") SchemaProviderConfiguration schema_provider,
                         @JsonProperty("drop_schema") boolean drop_schema,
                         @JsonProperty("create_keyspace") String keyspace_ddl,
                         @JsonProperty("create_schema") boolean create_schema,
                         @JsonProperty("truncate_schema") boolean truncate_table,
                         @JsonProperty("metric_reporter") MetricReporterConfiguration metric_reporter,
                         @JsonProperty("clock") ClockConfiguration clock,
                         @JsonProperty("runner") RunnerConfiguration runner,
                         @JsonProperty("system_under_test") SutConfiguration system_under_test,
                         @JsonProperty("data_tracker") DataTrackerConfiguration data_tracker,
                         @JsonProperty("partition_descriptor_selector") PDSelectorConfiguration partition_descriptor_selector,
                         @JsonProperty("clustering_descriptor_selector") CDSelectorConfiguration clustering_descriptor_selector)
    {
        this.seed = seed;
        this.schema_provider = schema_provider;
        this.keyspace_ddl = keyspace_ddl;
        this.drop_schema = drop_schema;
        this.create_schema = create_schema;
        this.truncate_table = truncate_table;
        this.metric_reporter = metric_reporter;
        this.clock = clock;
        this.system_under_test = system_under_test;
        this.data_tracker = data_tracker;
        this.partition_descriptor_selector = partition_descriptor_selector;
        this.clustering_descriptor_selector = clustering_descriptor_selector;
        this.runner = runner;
    }

    public static void registerSubtypes(Class<?>... classes)
    {
        mapper.registerSubtypes(classes);
    }

    public static void toFile(File file, Configuration config)
    {
        try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file))))
        {
            bw.write(Configuration.toYamlString(config));
            bw.flush();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
    public static String toYamlString(Configuration config)
    {
        try
        {
            return mapper.writeValueAsString(config);
        }
        catch (Throwable t)
        {
            throw new RuntimeException(t);
        }
    }

    public static Configuration fromYamlString(String config)
    {
        try
        {
            return mapper.readValue(config, Configuration.class);
        }
        catch (Throwable t)
        {
            throw new RuntimeException(t);
        }
    }

    public static Configuration fromFile(String path)
    {
        return fromFile(new File(path));
    }

    public static Configuration fromFile(File file)
    {
        try
        {
            return mapper.readValue(file, Configuration.class);
        }
        catch (Throwable t)
        {
            throw new RuntimeException(t);
        }
    }

    public static void validate(Configuration config)
    {
        Objects.requireNonNull(config.schema_provider, "Schema provider should not be null");
        Objects.requireNonNull(config.metric_reporter, "Metric reporter should not be null");
        Objects.requireNonNull(config.clock, "Clock should not be null");
        Objects.requireNonNull(config.system_under_test, "System under test should not be null");
        Objects.requireNonNull(config.partition_descriptor_selector, "Partition descriptor selector should not be null");
        Objects.requireNonNull(config.clustering_descriptor_selector, "Clustering descriptor selector should not be null");
    }

    public Runner createRunner()
    {
        return createRunner(this);
    }

    public Run createRun()
    {
        return createRun(this);
    }

    public static Run createRun(Configuration snapshot)
    {
        SystemUnderTest sut = null;
        try
        {
            validate(snapshot);

            long seed = snapshot.seed;

            OpSelectors.PureRng rng = new OpSelectors.PCGFast(seed);

            // TODO: validate that operation kind is compatible with schema, due to statics etc
            sut = snapshot.system_under_test.make();
            SchemaSpec schemaSpec = snapshot.schema_provider.make(seed, sut);
            schemaSpec.validate();

            OpSelectors.PdSelector pdSelector = snapshot.partition_descriptor_selector.make(rng);
            DataTrackerConfiguration dataTrackerConfiguration = snapshot.data_tracker == null ? new DefaultDataTrackerConfiguration() : snapshot.data_tracker;
            DataTracker tracker = dataTrackerConfiguration.make(pdSelector, schemaSpec);

            OpSelectors.DescriptorSelector descriptorSelector = snapshot.clustering_descriptor_selector.make(rng, schemaSpec);
            OpSelectors.Clock clock = snapshot.clock.make();

            MetricReporter metricReporter = snapshot.metric_reporter.make();

            return new Run(rng,
                           clock,
                           pdSelector,
                           descriptorSelector,
                           schemaSpec,
                           tracker,
                           sut,
                           metricReporter);
        }
        catch (Throwable t)
        {
            // Make sure to shut down all SUT threads if it has been started
            if (sut != null)
            {
                sut.shutdown();
            }
            throw t;
        }
    }

    public static Runner createRunner(Configuration config)
    {
        Run run = createRun(config);
        return config.runner.make(run, config);
    }

    public static class ConfigurationBuilder
    {
        long seed;
        SchemaProviderConfiguration schema_provider = new DefaultSchemaProviderConfiguration();

        String keyspace_ddl;
        boolean drop_schema;
        boolean create_schema;
        boolean truncate_table;

        ClockConfiguration clock;
        MetricReporterConfiguration metric_reporter = new NoOpMetricReporterConfiguration();
        DataTrackerConfiguration data_tracker = new DefaultDataTrackerConfiguration();
        RunnerConfiguration runner;
        SutConfiguration system_under_test;
        PDSelectorConfiguration partition_descriptor_selector = new Configuration.DefaultPDSelectorConfiguration(10, 100);
        CDSelectorConfiguration clustering_descriptor_selector; // TODO: sensible default value

        public ConfigurationBuilder setSeed(long seed)
        {
            this.seed = seed;
            return this;
        }

        public ConfigurationBuilder setSchemaProvider(SchemaProviderConfiguration schema_provider)
        {
            this.schema_provider = schema_provider;
            return this;
        }

        public ConfigurationBuilder setDataTracker(DataTrackerConfiguration tracker)
        {
            this.data_tracker = tracker;
            return this;
        }

        public ConfigurationBuilder setKeyspaceDdl(String keyspace_ddl)
        {
            this.keyspace_ddl = keyspace_ddl;
            return this;
        }


        public ConfigurationBuilder setClock(ClockConfiguration clock)
        {
            this.clock = clock;
            return this;
        }

        public ConfigurationBuilder setSUT(SutConfiguration system_under_test)
        {
            this.system_under_test = system_under_test;
            return this;
        }

        public ConfigurationBuilder setDropSchema(boolean drop_schema)
        {
            this.drop_schema = drop_schema;
            return this;
        }

        public ConfigurationBuilder setCreateSchema(boolean create_schema)
        {
            this.create_schema = create_schema;
            return this;
        }

        public ConfigurationBuilder setTruncateTable(boolean truncate_table)
        {
            this.truncate_table = truncate_table;
            return this;
        }

        public ConfigurationBuilder setRunner(RunnerConfiguration runner)
        {
            this.runner = runner;
            return this;
        }

        public ConfigurationBuilder setPartitionDescriptorSelector(PDSelectorConfiguration partition_descriptor_selector)
        {
            this.partition_descriptor_selector = partition_descriptor_selector;
            return this;
        }

        public ConfigurationBuilder setClusteringDescriptorSelector(CDSelectorConfiguration builder)
        {
            this.clustering_descriptor_selector = builder;
            return this;
        }

        public ConfigurationBuilder setClusteringDescriptorSelector(Consumer<CDSelectorConfigurationBuilder> build)
        {
            CDSelectorConfigurationBuilder builder = new CDSelectorConfigurationBuilder();
            build.accept(builder);
            return setClusteringDescriptorSelector(builder.build());
        }

        public ConfigurationBuilder setMetricReporter(MetricReporterConfiguration metric_reporter)
        {
            this.metric_reporter = metric_reporter;
            return this;
        }

        public Configuration build()
        {
            return new Configuration(seed,
                                     schema_provider,
                                     drop_schema,
                                     keyspace_ddl,
                                     create_schema,
                                     truncate_table,
                                     metric_reporter,
                                     clock,
                                     runner,
                                     system_under_test,
                                     data_tracker,
                                     partition_descriptor_selector,
                                     clustering_descriptor_selector);
        }
    }

    public ConfigurationBuilder unbuild()
    {
        ConfigurationBuilder builder = new ConfigurationBuilder();
        builder.seed = seed;

        builder.schema_provider = schema_provider;
        builder.keyspace_ddl = keyspace_ddl;
        builder.drop_schema = drop_schema;
        builder.create_schema = create_schema;
        builder.truncate_table = truncate_table;

        builder.data_tracker = data_tracker;
        builder.clock = clock;
        builder.runner = runner;
        builder.system_under_test = system_under_test;
        builder.metric_reporter = metric_reporter;

        builder.partition_descriptor_selector = partition_descriptor_selector;
        builder.clustering_descriptor_selector = clustering_descriptor_selector;

        return builder;
    }


    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT)
    public interface DataTrackerConfiguration extends DataTracker.DataTrackerFactory
    {

    }

    @JsonTypeName("no_op")
    public static class NoOpDataTrackerConfiguration implements DataTrackerConfiguration
    {
        @JsonCreator
        public NoOpDataTrackerConfiguration()
        {
        }

        public DataTracker make(OpSelectors.PdSelector pdSelector, SchemaSpec schemaSpec)
        {
            return DataTracker.NO_OP;
        }
    }

    @JsonTypeName("default")
    public static class DefaultDataTrackerConfiguration implements DataTrackerConfiguration
    {
        public final long max_seen_lts;
        public final long max_complete_lts;
        public final List<Long> reorder_buffer;

        public DefaultDataTrackerConfiguration()
        {
            this(-1, -1, null);
        }

        @JsonCreator
        public DefaultDataTrackerConfiguration(@JsonProperty(value = "max_seen_lts", defaultValue = "-1") long max_seen_lts,
                                               @JsonProperty(value = "max_complete_lts", defaultValue = "-1") long max_complete_lts,
                                               @JsonProperty(value = "reorder_buffer", defaultValue = "null") List<Long> reorder_buffer)
        {
            this.max_seen_lts = max_seen_lts;
            this.max_complete_lts = max_complete_lts;
            this.reorder_buffer = reorder_buffer;
        }

        @Override
        public DataTracker make(OpSelectors.PdSelector pdSelector, SchemaSpec schemaSpec)
        {
            DefaultDataTracker tracker = new DefaultDataTracker();
            tracker.forceLts(max_seen_lts, max_complete_lts, reorder_buffer);
            return tracker;
        }
    }

    @JsonTypeName("locking")
    public static class LockingDataTrackerConfiguration implements DataTrackerConfiguration
    {
        public final long max_seen_lts;
        public final long max_complete_lts;
        public final List<Long> reorder_buffer;

        @JsonCreator
        public LockingDataTrackerConfiguration(@JsonProperty(value = "max_seen_lts", defaultValue = "-1") long max_seen_lts,
                                               @JsonProperty(value = "max_complete_lts", defaultValue = "-1") long max_complete_lts,
                                               @JsonProperty(value = "reorder_buffer", defaultValue = "null") List<Long> reorder_buffer)
        {
            this.max_seen_lts = max_seen_lts;
            this.max_complete_lts = max_complete_lts;
            this.reorder_buffer = reorder_buffer;
        }

        @Override
        public DataTracker make(OpSelectors.PdSelector pdSelector, SchemaSpec schemaSpec)
        {
            LockingDataTracker tracker = new LockingDataTracker(pdSelector, schemaSpec);
            tracker.forceLts(max_seen_lts, max_complete_lts, reorder_buffer);
            return tracker;
        }
    }

    public static class DefaultLockingDataTrackerConfiguration implements DataTrackerConfiguration
    {
        @Override
        public DataTracker make(OpSelectors.PdSelector pdSelector, SchemaSpec schemaSpec)
        {
            LockingDataTracker tracker = new LockingDataTracker(pdSelector, schemaSpec);
            tracker.forceLts(-1, -1, null);
            return tracker;
        }
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT)
    public interface ClockConfiguration extends OpSelectors.ClockFactory
    {
    }

    @JsonTypeName("approximate_monotonic")
    public static class ApproximateClockConfiguration implements ClockConfiguration
    {
        public final int history_size;
        public final int epoch_length;
        public final TimeUnit epoch_time_unit;

        @JsonCreator
        public ApproximateClockConfiguration(@JsonProperty("history_size") int history_size,
                                             @JsonProperty("epoch_length") int epoch_length,
                                             @JsonProperty("epoch_time_unit") TimeUnit epoch_time_unit)
        {
            this.history_size = history_size;
            this.epoch_length = epoch_length;
            this.epoch_time_unit = epoch_time_unit;
        }

        public OpSelectors.Clock make()
        {
            return new ApproximateClock(history_size,
                                        epoch_length,
                                        epoch_time_unit);
        }
    }

    @JsonTypeName("debug_approximate_monotonic")
    public static class DebugApproximateClockConfiguration implements ClockConfiguration
    {
        public final long start_time_micros;
        public final int history_size;
        public final long[] history;
        public final long lts;
        public final int idx;
        public final long epoch_period;
        public final TimeUnit epoch_time_unit;

        @JsonCreator
        public DebugApproximateClockConfiguration(@JsonProperty("start_time_micros") long start_time_micros,
                                                  @JsonProperty("history_size") int history_size,
                                                  @JsonProperty("history") long[] history,
                                                  @JsonProperty("lts") long lts,
                                                  @JsonProperty("idx") int idx,
                                                  @JsonProperty("epoch_period") long epoch_period,
                                                  @JsonProperty("epoch_time_unit") TimeUnit epoch_time_unit)
        {
            this.start_time_micros = start_time_micros;
            this.history_size = history_size;
            this.history = history;
            this.lts = lts;
            this.idx = idx;
            this.epoch_period = epoch_period;
            this.epoch_time_unit = epoch_time_unit;
        }

        public OpSelectors.Clock make()
        {
            return ApproximateClock.forDebug(start_time_micros,
                                             history_size,
                                             lts,
                                             idx,
                                             epoch_period,
                                             epoch_time_unit,
                                             history);
        }
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT)
    public interface RunnerConfiguration extends Runner.RunnerFactory
    {
    }

    @JsonTypeName("concurrent")
    public static class ConcurrentRunnerConfig implements RunnerConfiguration
    {
        @JsonProperty(value = "visitor_pools")
        public final List<VisitorPoolConfiguration> visitor_pools;

        public final long run_time;
        public final TimeUnit run_time_unit;

        @JsonCreator
        public ConcurrentRunnerConfig(@JsonProperty(value = "visitor_pools") List<VisitorPoolConfiguration> visitor_pools,
                                      @JsonProperty(value = "run_time", defaultValue = "2") long runtime,
                                      @JsonProperty(value = "run_time_unit", defaultValue = "HOURS") TimeUnit runtimeUnit)
        {
            this.visitor_pools = visitor_pools;
            this.run_time = runtime;
            this.run_time_unit = runtimeUnit;
        }

        @Override
        public Runner make(Run run, Configuration config)
        {
            return new Runner.ConcurrentRunner(run, config, visitor_pools, run_time, run_time_unit);
        }
    }

    public static class VisitorPoolConfiguration
    {
        public final String prefix;
        public final int concurrency;
        public final VisitorConfiguration visitor;

        @JsonCreator
        public VisitorPoolConfiguration(@JsonProperty(value = "prefix") String prefix,
                                        @JsonProperty(value = "concurrency") int concurrency,
                                        @JsonProperty(value = "visitor") VisitorConfiguration visitor)
        {
            this.prefix = prefix;
            this.concurrency = concurrency;
            this.visitor = visitor;
        }

        public static VisitorPoolConfiguration pool(String prefix, int concurrency, VisitorConfiguration visitor)
        {
            return new VisitorPoolConfiguration(prefix, concurrency, visitor);
        }
    }

    @JsonTypeName("sequential")
    public static class SequentialRunnerConfig implements RunnerConfiguration
    {
        @JsonProperty(value = "visitors")
        public final List<VisitorConfiguration> visitorFactories;

        public final long run_time;
        public final TimeUnit run_time_unit;

        @JsonCreator
        public SequentialRunnerConfig(@JsonProperty(value = "visitors") List<VisitorConfiguration> visitors,
                                      @JsonProperty(value = "run_time", defaultValue = "2") long runtime,
                                      @JsonProperty(value = "run_time_unit", defaultValue = "HOURS") TimeUnit runtimeUnit)
        {
            this.visitorFactories = visitors;
            this.run_time = runtime;
            this.run_time_unit = runtimeUnit;
        }

        @Override
        public Runner make(Run run, Configuration config)
        {
            return new Runner.SequentialRunner(run, config, visitorFactories, run_time, run_time_unit);
        }
    }

    @JsonTypeName("single")
    public static class SingleVisitRunnerConfig implements RunnerConfiguration
    {
        @JsonProperty(value = "visitors")
        public final List<VisitorConfiguration> visitorFactories;

        @JsonCreator
        public SingleVisitRunnerConfig(@JsonProperty(value = "visitors") List<VisitorConfiguration> visitors)
        {
            this.visitorFactories = visitors;
        }

        @Override
        public Runner make(Run run, Configuration config)
        {
            return new Runner.SingleVisitRunner(run, config, visitorFactories);
        }
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT)
    public interface SutConfiguration extends SystemUnderTest.SUTFactory
    {
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT)
    public interface ModelConfiguration extends Model.ModelFactory
    {
    }

    @JsonTypeName("quiescent_checker")
    public static class QuiescentCheckerConfig implements ModelConfiguration
    {
        @JsonCreator
        public QuiescentCheckerConfig()
        {
        }

        public Model make(Run run)
        {
            return new QuiescentChecker(run);
        }
    }

    @JsonTypeName("no_op")
    public static class NoOpCheckerConfig implements ModelConfiguration
    {
        @JsonCreator
        public NoOpCheckerConfig()
        {
        }

        public Model make(Run run)
        {
            return new NoOpChecker(run);
        }
    }


    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT)
    public interface PDSelectorConfiguration extends OpSelectors.PdSelectorFactory
    {
    }

    @JsonTypeName("default")
    public static class DefaultPDSelectorConfiguration implements PDSelectorConfiguration
    {
        public final int window_size;
        public final int slide_after_repeats;
        public final long position_offset;
        public final long position_window_size;

        public DefaultPDSelectorConfiguration(int window_size,
                                              int slide_after_repeats)
        {
            this.window_size = window_size;
            this.slide_after_repeats = slide_after_repeats;
            this.position_offset = 0L;
            this.position_window_size = Long.MAX_VALUE;
        }

        @JsonCreator
        public DefaultPDSelectorConfiguration(@JsonProperty(value = "window_size", defaultValue = "10") int window_size,
                                              @JsonProperty(value = "slide_after_repeats", defaultValue = "100") int slide_after_repeats,
                                              @JsonProperty(value = "runner_index") Long runner_index,
                                              @JsonProperty(value = "total_runners") Long total_runners,
                                              @JsonProperty(value = "position_offset") Long position_offset,
                                              @JsonProperty(value = "position_window_size") Long position_window_size)
        {
            this.window_size = window_size;
            this.slide_after_repeats = slide_after_repeats;
            if (runner_index != null || total_runners != null)
            {
                assert runner_index != null && total_runners != null : "Both runner_index and total_runners are required";
                assert position_offset == null && position_window_size == null : "Please use either runner_index/total_runners or position_offset/position_window_size combinations.";
                this.position_window_size = Long.MAX_VALUE / total_runners;
                this.position_offset = this.position_window_size * runner_index;
            }
            else
            {
                assert runner_index == null && total_runners == null : "Please use either runner_index/total_runners or position_offset/position_window_size combinations.";
                this.position_offset = position_offset == null ? 0 : position_offset;
                if (position_window_size == null)
                    this.position_window_size = Long.MAX_VALUE - this.position_offset;
                else
                    this.position_window_size = position_window_size;
            }
        }

        public OpSelectors.PdSelector make(OpSelectors.PureRng rng)
        {
            return new OpSelectors.DefaultPdSelector(rng, window_size, slide_after_repeats, position_offset, position_window_size);
        }
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT)
    public interface CDSelectorConfiguration extends OpSelectors.DescriptorSelectorFactory
    {
    }

    public static class WeightedSelectorBuilder<T>
    {
        private final Map<T, Integer> operation_kind_weights;

        public WeightedSelectorBuilder()
        {
            operation_kind_weights = new HashMap<>();
        }

        public WeightedSelectorBuilder<T> addWeight(T v, int weight)
        {
            operation_kind_weights.put(v, weight);
            return this;
        }

        public Map<T, Integer> build()
        {
            return operation_kind_weights;
        }
    }

    public static class OperationKindSelectorBuilder extends WeightedSelectorBuilder<OpSelectors.OperationKind>
    {
    }

    // TODO: configure fractions/fractional builder
    public static class CDSelectorConfigurationBuilder
    {
        private DistributionConfig operations_per_lts = new ConstantDistributionConfig(10);
        private int max_partition_size = 100;
        private Map<OpSelectors.OperationKind, Integer> operation_kind_weights = new OperationKindSelectorBuilder()
                                                                                 .addWeight(OpSelectors.OperationKind.DELETE_ROW, 1)
                                                                                 .addWeight(OpSelectors.OperationKind.DELETE_COLUMN, 1)
                                                                                 .addWeight(OpSelectors.OperationKind.INSERT, 98)
                                                                                 .build();
        private Map<OpSelectors.OperationKind, long[]> column_mask_bitsets;
        private int[] fractions;

        public CDSelectorConfigurationBuilder setOperationsPerLtsDistribution(DistributionConfig operations_per_lts)
        {
            this.operations_per_lts = operations_per_lts;
            return this;
        }

        public CDSelectorConfigurationBuilder setMaxPartitionSize(int max_partition_size)
        {
            if (max_partition_size <= 0)
                throw new IllegalArgumentException("Max partition size should be positive");
            this.max_partition_size = max_partition_size;
            return this;
        }

        public CDSelectorConfigurationBuilder setOperationKindWeights(Map<OpSelectors.OperationKind, Integer> operation_kind_weights)
        {
            this.operation_kind_weights = operation_kind_weights;
            return this;
        }

        public CDSelectorConfigurationBuilder setColumnMasks(Map<OpSelectors.OperationKind, long[]> column_mask_bitsets)
        {
            this.column_mask_bitsets = column_mask_bitsets;
            return this;
        }

        public CDSelectorConfigurationBuilder setFractions(int[] fractions)
        {
            this.fractions = fractions;
            return this;
        }

        public DefaultCDSelectorConfiguration build()
        {
            if (fractions == null)
            {
                return new DefaultCDSelectorConfiguration(operations_per_lts,
                                                          max_partition_size,
                                                          operation_kind_weights,
                                                          column_mask_bitsets);
            }
            else
            {
                return new HierarchicalCDSelectorConfiguration(operations_per_lts,
                                                               max_partition_size,
                                                               operation_kind_weights,
                                                               column_mask_bitsets,
                                                               fractions);
            }
        }
    }

    @JsonTypeName("default")
    public static class DefaultCDSelectorConfiguration implements CDSelectorConfiguration
    {
        public final DistributionConfig operations_per_lts;
        public final int max_partition_size;
        public final Map<OpSelectors.OperationKind, Integer> operation_kind_weights;
        public final Map<OpSelectors.OperationKind, long[]> column_mask_bitsets;

        @JsonCreator
        public DefaultCDSelectorConfiguration(@JsonProperty("operations_per_lts") DistributionConfig operations_per_lts,
                                              @JsonProperty(value = "window_size", defaultValue = "100") int max_partition_size,
                                              @JsonProperty("operation_kind_weights") Map<OpSelectors.OperationKind, Integer> operation_kind_weights,
                                              @JsonProperty("column_mask_bitsets") Map<OpSelectors.OperationKind, long[]> column_mask_bitsets)
        {
            this.operations_per_lts = operations_per_lts;
            this.max_partition_size = max_partition_size;
            this.operation_kind_weights = operation_kind_weights;
            this.column_mask_bitsets = column_mask_bitsets;
        }

        protected OpSelectors.ColumnSelector columnSelector(SchemaSpec schemaSpec)
        {
            OpSelectors.ColumnSelector columnSelector;
            if (column_mask_bitsets == null)
            {
                columnSelector = OpSelectors.columnSelectorBuilder().forAll(schemaSpec).build();
            }
            else
            {
                Map<OpSelectors.OperationKind, Surjections.Surjection<BitSet>> m = new EnumMap<>(OpSelectors.OperationKind.class);
                for (Map.Entry<OpSelectors.OperationKind, long[]> entry : column_mask_bitsets.entrySet())
                {
                    List<BitSet> bitSets = new ArrayList<>(entry.getValue().length);
                    for (long raw_bitset : entry.getValue())
                    {
                        bitSets.add(BitSet.create(raw_bitset, schemaSpec.allColumns.size()));
                    }
                    Surjections.Surjection<BitSet> selector = Surjections.pick(bitSets);
                    m.put(entry.getKey(), selector);
                }
                columnSelector = (opKind, descr) -> m.get(opKind).inflate(descr);
            }

            return columnSelector;
        }

        public OpSelectors.DescriptorSelector make(OpSelectors.PureRng rng, SchemaSpec schemaSpec)
        {
            return new OpSelectors.DefaultDescriptorSelector(rng,
                                                             columnSelector(schemaSpec),
                                                             OpSelectors.OperationSelector.weighted(operation_kind_weights),
                                                             operations_per_lts.make(),
                                                             max_partition_size);
        }
    }

    public static class HierarchicalCDSelectorConfiguration extends DefaultCDSelectorConfiguration
    {
        private final int[] fractions;

        public HierarchicalCDSelectorConfiguration(DistributionConfig operations_per_lts,
                                                   int max_partition_size,
                                                   Map<OpSelectors.OperationKind, Integer> operation_kind_weights,
                                                   Map<OpSelectors.OperationKind, long[]> column_mask_bitsets,
                                                   int[] fractions)
        {
            super(operations_per_lts, max_partition_size, operation_kind_weights, column_mask_bitsets);
            this.fractions = fractions;
        }

        public OpSelectors.DescriptorSelector make(OpSelectors.PureRng rng, SchemaSpec schemaSpec)
        {
            return new OpSelectors.HierarchicalDescriptorSelector(rng,
                                                                  fractions,
                                                                  columnSelector(schemaSpec),
                                                                  OpSelectors.OperationSelector.weighted(operation_kind_weights),
                                                                  operations_per_lts.make(),
                                                                  max_partition_size);
        }
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
    public interface DistributionConfig extends Distribution.DistributionFactory
    {
    }

    @JsonTypeName("identity")
    public static class IdentityDistributionConfig implements DistributionConfig
    {
        @JsonCreator
        public IdentityDistributionConfig()
        {
        }

        public Distribution make()
        {
            return new Distribution.IdentityDistribution();
        }
    }

    @JsonTypeName("normal")
    public static class NormalDistributionConfig implements DistributionConfig
    {
        @JsonCreator
        public NormalDistributionConfig()
        {
        }

        public Distribution make()
        {
            return new Distribution.NormalDistribution();
        }
    }

    @JsonTypeName("constant")
    public static class ConstantDistributionConfig implements DistributionConfig
    {
        public final long constant;

        @JsonCreator
        public ConstantDistributionConfig(@JsonProperty("constant") long constant)
        {
            this.constant = constant;
        }

        public Distribution make()
        {
            return new Distribution.ConstantDistribution(constant);
        }
    }

    @JsonTypeName("scaled")
    public static class ScaledDistributionConfig implements DistributionConfig
    {
        private final long min;
        private final long max;

        @JsonCreator
        public ScaledDistributionConfig(long min, long max)
        {
            this.min = min;
            this.max = max;
        }

        public Distribution make()
        {
            return new Distribution.ScaledDistribution(min, max);
        }
    }


    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT)
    public interface VisitorConfiguration extends Visitor.VisitorFactory
    {
    }


    @JsonTypeName("mutating")
    public static class MutatingVisitorConfiguation implements VisitorConfiguration
    {
        public final RowVisitorConfiguration row_visitor;

        @JsonCreator
        public MutatingVisitorConfiguation(@JsonProperty("row_visitor") RowVisitorConfiguration row_visitor)
        {
            this.row_visitor = row_visitor;
        }

        @Override
        public Visitor make(Run run)
        {
            return new MutatingVisitor(run, row_visitor::make);
        }
    }

    @JsonTypeName("logging")
    public static class LoggingVisitorConfiguration implements VisitorConfiguration
    {
        protected final RowVisitorConfiguration row_visitor;

        @JsonCreator
        public LoggingVisitorConfiguration(@JsonProperty("row_visitor") RowVisitorConfiguration row_visitor)
        {
            this.row_visitor = row_visitor;
        }

        @Override
        public Visitor make(Run run)
        {
            return new LoggingVisitor(run, row_visitor::make);
        }
    }

    @JsonTypeName("validate_all_partitions")
    public static class AllPartitionsValidatorConfiguration implements VisitorConfiguration
    {
        public final int concurrency;
        public final QueryLoggerConfiguration query_logger;
        public final Configuration.ModelConfiguration model;

        @JsonCreator
        public AllPartitionsValidatorConfiguration(@JsonProperty("concurrency") int concurrency,
                                                   @JsonProperty("model") Configuration.ModelConfiguration model,
                                                   @JsonProperty("query_logger") QueryLoggerConfiguration query_logger)
        {
            this.concurrency = concurrency;
            this.model = model;
            this.query_logger = QueryLogger.thisOrDefault(query_logger);
        }

        public Visitor make(Run run)
        {
            return new AllPartitionsValidator(run, concurrency, model, query_logger.make());
        }
    }

    @JsonTypeName("corrupt")
    public static class CorruptingVisitorConfiguration implements VisitorConfiguration
    {
        public final int trigger_after;

        @JsonCreator
        public CorruptingVisitorConfiguration(@JsonProperty("trigger_after") int trigger_after)
        {
            this.trigger_after = trigger_after;
        }

        public Visitor make(Run run)
        {
            return new CorruptingVisitor(trigger_after, run);
        }
    }

    @JsonTypeName("validate_recent_partitions")
    public static class RecentPartitionsValidatorConfiguration implements VisitorConfiguration
    {
        public final int partition_count;
        public final int queries;
        public final Configuration.ModelConfiguration modelConfiguration;
        public final QueryLoggerConfiguration query_logger;

        // TODO: make query selector configurable
        @JsonCreator
        public RecentPartitionsValidatorConfiguration(@JsonProperty("partition_count") int partition_count,
                                                      @JsonProperty("queries_per_partition") int queries,
                                                      @JsonProperty("model") Configuration.ModelConfiguration model,
                                                      @JsonProperty("logger") QueryLoggerConfiguration query_logger)
        {
            this.partition_count = partition_count;
            this.queries = queries;
            this.modelConfiguration = model;
            this.query_logger = QueryLogger.thisOrDefault(query_logger);
        }

        @Override
        public Visitor make(Run run)
        {
            return new RecentValidator(partition_count, queries, run, modelConfiguration, query_logger.make());
        }
    }

    @JsonTypeName("validate_random_partitions")
    public static class RandomPartitionValidatorConfiguration implements VisitorConfiguration
    {
        public final int partition_count;
        public final int queries;
        public final Configuration.ModelConfiguration model_configuration;
        public final QueryLoggerConfiguration query_logger;

        // TODO: make query selector configurable
        @JsonCreator
        public RandomPartitionValidatorConfiguration(@JsonProperty("partition_count") int partition_count,
                                                     @JsonProperty("queries_per_partition") int queries,
                                                     @JsonProperty("model") Configuration.ModelConfiguration model,
                                                     @JsonProperty("logger") QueryLoggerConfiguration query_logger)
        {
            this.partition_count = partition_count;
            this.queries = queries;
            this.model_configuration = model;
            this.query_logger = QueryLogger.thisOrDefault(query_logger);
        }

        @Override
        public Visitor make(Run run)
        {
            return new RandomValidator(partition_count, queries, run, model_configuration, query_logger.make());
        }
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT)
    public interface QueryLoggerConfiguration extends QueryLogger.QueryLoggerFactory
    {
    }

    @JsonTypeName("no_op")
    public static class NoOpQueryLoggerConfiguration implements QueryLoggerConfiguration
    {
        public QueryLogger make()
        {
            return QueryLogger.NO_OP;
        }
    }

    @JsonTypeName("file")
    public static class FileQueryLoggerConfiguration implements QueryLoggerConfiguration
    {
        public final String filename;

        @JsonCreator
        public FileQueryLoggerConfiguration(@JsonProperty("filename") String filename)
        {
            this.filename = filename;
        }

        public QueryLogger make()
        {
            return new QueryLogger.FileQueryLogger(filename);
        }
    }


    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT)
    public interface RowVisitorConfiguration extends OperationExecutor.RowVisitorFactory
    {
    }

    @JsonTypeName("mutating")
    public static class MutatingRowVisitorConfiguration implements RowVisitorConfiguration
    {
        @Override
        public OperationExecutor make(Run run)
        {
            return new MutatingRowVisitor(run);
        }
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT)
    public interface SchemaProviderConfiguration extends SchemaSpec.SchemaSpecFactory
    {
    }

    @JsonTypeName("default")
    public static class DefaultSchemaProviderConfiguration implements SchemaProviderConfiguration
    {
        public SchemaSpec make(long seed, SystemUnderTest sut)
        {
            return SchemaGenerators.defaultSchemaSpecGen("table0")
                                   .inflate(seed);
        }
    }

    @JsonTypeName("fixed")
    public static class FixedSchemaProviderConfiguration implements SchemaProviderConfiguration
    {
        private final SchemaSpec schemaSpec;

        @JsonCreator
        public FixedSchemaProviderConfiguration(@JsonProperty("keyspace") String keyspace,
                                                @JsonProperty("table") String table,
                                                @JsonProperty("partition_keys") Map<String, String> pks,
                                                @JsonProperty("clustering_keys") Map<String, String> cks,
                                                @JsonProperty("regular_columns") Map<String, String> regulars,
                                                @JsonProperty("static_columns") Map<String, String> statics)
        {
            this(SchemaGenerators.parse(keyspace, table,
                                        pks, cks, regulars, statics));
        }

        public FixedSchemaProviderConfiguration(SchemaSpec schemaSpec)
        {
            this.schemaSpec = schemaSpec;
        }
        public SchemaSpec make(long seed, SystemUnderTest sut)
        {
            return schemaSpec;
        }
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT)
    public interface MetricReporterConfiguration extends MetricReporter.MetricReporterFactory
    {
    }

    @JsonTypeName("no_op")
    public static class NoOpMetricReporterConfiguration implements MetricReporterConfiguration
    {
        public MetricReporter make()
        {
            return MetricReporter.NO_OP;
        }
    }
}
