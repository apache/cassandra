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
package org.apache.cassandra.cql3;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.*;
import com.datastax.driver.core.ResultSet;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.cql3.functions.ThreadAwareSecurityManager;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.transport.Event;
import org.apache.cassandra.transport.Server;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;

import static junit.framework.Assert.assertNotNull;

/**
 * Base class for CQL tests.
 */
public abstract class CQLTester
{
    protected static final Logger logger = LoggerFactory.getLogger(CQLTester.class);

    public static final String KEYSPACE = "cql_test_keyspace";
    public static final String KEYSPACE_PER_TEST = "cql_test_keyspace_alt";
    protected static final boolean USE_PREPARED_VALUES = Boolean.valueOf(System.getProperty("cassandra.test.use_prepared", "true"));
    protected static final boolean REUSE_PREPARED = Boolean.valueOf(System.getProperty("cassandra.test.reuse_prepared", "true"));
    protected static final long ROW_CACHE_SIZE_IN_MB = Integer.valueOf(System.getProperty("cassandra.test.row_cache_size_in_mb", "0"));
    private static final AtomicInteger seqNumber = new AtomicInteger();

    private static org.apache.cassandra.transport.Server server;
    protected static final int nativePort;
    protected static final InetAddress nativeAddr;
    private static final Map<Integer, Cluster> clusters = new HashMap<>();
    private static final Map<Integer, Session> sessions = new HashMap<>();

    private static boolean isServerPrepared = false;

    public static final List<Integer> PROTOCOL_VERSIONS;
    static
    {
        // The latest versions might not be supported yet by the java driver
        ImmutableList.Builder<Integer> builder = ImmutableList.builder();
        for (int version = Server.MIN_SUPPORTED_VERSION; version <= Server.CURRENT_VERSION; version++)
        {
            try
            {
                ProtocolVersion.fromInt(version);
                builder.add(version);
            }
            catch (IllegalArgumentException e)
            {
                break;
            }
        }
        PROTOCOL_VERSIONS = builder.build();

        // Once per-JVM is enough
        prepareServer();

        nativeAddr = InetAddress.getLoopbackAddress();

        try
        {
            try (ServerSocket serverSocket = new ServerSocket(0))
            {
                nativePort = serverSocket.getLocalPort();
            }
            Thread.sleep(250);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public static ResultMessage lastSchemaChangeResult;

    private List<String> tables = new ArrayList<>();
    private List<String> types = new ArrayList<>();
    private List<String> functions = new ArrayList<>();
    private List<String> aggregates = new ArrayList<>();

    // We don't use USE_PREPARED_VALUES in the code below so some test can foce value preparation (if the result
    // is not expected to be the same without preparation)
    private boolean usePrepared = USE_PREPARED_VALUES;
    private static boolean reusePrepared = REUSE_PREPARED;

    protected boolean usePrepared()
    {
        return usePrepared;
    }

    public static void prepareServer()
    {
        if (isServerPrepared)
            return;

        // Cleanup first
        try
        {
            cleanupAndLeaveDirs();
        }
        catch (IOException e)
        {
            logger.error("Failed to cleanup and recreate directories.");
            throw new RuntimeException(e);
        }

        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler()
        {
            public void uncaughtException(Thread t, Throwable e)
            {
                logger.error("Fatal exception in thread " + t, e);
            }
        });

        ThreadAwareSecurityManager.install();

        Keyspace.setInitialized();
        isServerPrepared = true;
    }

    public static void cleanupAndLeaveDirs() throws IOException
    {
        // We need to stop and unmap all CLS instances prior to cleanup() or we'll get failures on Windows.
        CommitLog.instance.stopUnsafe(true);
        mkdirs();
        cleanup();
        mkdirs();
        CommitLog.instance.restartUnsafe();
    }

    public static void cleanup()
    {
        // clean up commitlog
        String[] directoryNames = { DatabaseDescriptor.getCommitLogLocation(), };
        for (String dirName : directoryNames)
        {
            File dir = new File(dirName);
            if (!dir.exists())
                throw new RuntimeException("No such directory: " + dir.getAbsolutePath());
            FileUtils.deleteRecursive(dir);
        }

        cleanupSavedCaches();

        // clean up data directory which are stored as data directory/keyspace/data files
        for (String dirName : DatabaseDescriptor.getAllDataFileLocations())
        {
            File dir = new File(dirName);
            if (!dir.exists())
                throw new RuntimeException("No such directory: " + dir.getAbsolutePath());
            FileUtils.deleteRecursive(dir);
        }
    }

    public static void mkdirs()
    {
        DatabaseDescriptor.createAllDirectories();
    }

    public static void cleanupSavedCaches()
    {
        File cachesDir = new File(DatabaseDescriptor.getSavedCachesLocation());

        if (!cachesDir.exists() || !cachesDir.isDirectory())
            return;

        FileUtils.delete(cachesDir.listFiles());
    }

    @BeforeClass
    public static void setUpClass()
    {
        if (ROW_CACHE_SIZE_IN_MB > 0)
            DatabaseDescriptor.setRowCacheSizeInMB(ROW_CACHE_SIZE_IN_MB);

        StorageService.instance.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    @AfterClass
    public static void tearDownClass()
    {
        for (Session sess : sessions.values())
                sess.close();
        for (Cluster cl : clusters.values())
                cl.close();

        if (server != null)
            server.stop();

        // We use queryInternal for CQLTester so prepared statement will populate our internal cache (if reusePrepared is used; otherwise prepared
        // statements are not cached but re-prepared every time). So we clear the cache between test files to avoid accumulating too much.
        if (reusePrepared)
            QueryProcessor.clearInternalStatementsCache();
    }

    @Before
    public void beforeTest() throws Throwable
    {
        schemaChange(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}", KEYSPACE));
        schemaChange(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}", KEYSPACE_PER_TEST));
    }

    @After
    public void afterTest() throws Throwable
    {
        dropPerTestKeyspace();

        // Restore standard behavior in case it was changed
        usePrepared = USE_PREPARED_VALUES;
        reusePrepared = REUSE_PREPARED;

        final List<String> tablesToDrop = copy(tables);
        final List<String> typesToDrop = copy(types);
        final List<String> functionsToDrop = copy(functions);
        final List<String> aggregatesToDrop = copy(aggregates);
        tables = null;
        types = null;
        functions = null;
        aggregates = null;

        // We want to clean up after the test, but dropping a table is rather long so just do that asynchronously
        ScheduledExecutors.optionalTasks.execute(new Runnable()
        {
            public void run()
            {
                try
                {
                    for (int i = tablesToDrop.size() - 1; i >= 0; i--)
                        schemaChange(String.format("DROP TABLE IF EXISTS %s.%s", KEYSPACE, tablesToDrop.get(i)));

                    for (int i = aggregatesToDrop.size() - 1; i >= 0; i--)
                        schemaChange(String.format("DROP AGGREGATE IF EXISTS %s", aggregatesToDrop.get(i)));

                    for (int i = functionsToDrop.size() - 1; i >= 0; i--)
                        schemaChange(String.format("DROP FUNCTION IF EXISTS %s", functionsToDrop.get(i)));

                    for (int i = typesToDrop.size() - 1; i >= 0; i--)
                        schemaChange(String.format("DROP TYPE IF EXISTS %s.%s", KEYSPACE, typesToDrop.get(i)));

                    // Dropping doesn't delete the sstables. It's not a huge deal but it's cleaner to cleanup after us
                    // Thas said, we shouldn't delete blindly before the TransactionLogs.SSTableTidier for the table we drop
                    // have run or they will be unhappy. Since those taks are scheduled on StorageService.tasks and that's
                    // mono-threaded, just push a task on the queue to find when it's empty. No perfect but good enough.

                    final CountDownLatch latch = new CountDownLatch(1);
                    ScheduledExecutors.nonPeriodicTasks.execute(new Runnable()
                    {
                        public void run()
                        {
                            latch.countDown();
                        }
                    });
                    latch.await(2, TimeUnit.SECONDS);

                    removeAllSSTables(KEYSPACE, tablesToDrop);
                }
                catch (Exception e)
                {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    // lazy initialization for all tests that require Java Driver
    protected static void requireNetwork() throws ConfigurationException
    {
        if (server != null)
            return;

        SystemKeyspace.finishStartup();
        StorageService.instance.initServer();
        SchemaLoader.startGossiper();

        server = new Server.Builder().withHost(nativeAddr).withPort(nativePort).build();
        server.start();

        for (int version : PROTOCOL_VERSIONS)
        {
            if (clusters.containsKey(version))
                continue;

            Cluster cluster = Cluster.builder()
                                     .addContactPoints(nativeAddr)
                                     .withClusterName("Test Cluster")
                                     .withPort(nativePort)
                                     .withProtocolVersion(ProtocolVersion.fromInt(version))
                                     .build();
            clusters.put(version, cluster);
            sessions.put(version, cluster.connect());

            logger.info("Started Java Driver instance for protocol version {}", version);
        }
    }

    protected void dropPerTestKeyspace() throws Throwable
    {
        execute(String.format("DROP KEYSPACE IF EXISTS %s", KEYSPACE_PER_TEST));
    }

    /**
     * Returns a copy of the specified list.
     * @return a copy of the specified list.
     */
    private static List<String> copy(List<String> list)
    {
        return list.isEmpty() ? Collections.<String>emptyList() : new ArrayList<>(list);
    }

    public ColumnFamilyStore getCurrentColumnFamilyStore()
    {
        String currentTable = currentTable();
        return currentTable == null
             ? null
             : Keyspace.open(KEYSPACE).getColumnFamilyStore(currentTable);
    }

    public void flush(boolean forceFlush)
    {
        if (forceFlush)
            flush();
    }

    public void flush()
    {
        ColumnFamilyStore store = getCurrentColumnFamilyStore();
        if (store != null)
            store.forceBlockingFlush();
    }

    public void disableCompaction()
    {
        ColumnFamilyStore store = getCurrentColumnFamilyStore();
        store.disableAutoCompaction();
    }

    public void compact()
    {
        try
        {
            String currentTable = currentTable();
            if (currentTable != null)
                Keyspace.open(KEYSPACE).getColumnFamilyStore(currentTable).forceMajorCompaction();
        }
        catch (InterruptedException | ExecutionException e)
        {
            throw new RuntimeException(e);
        }
    }

    public void cleanupCache()
    {
        String currentTable = currentTable();
        if (currentTable != null)
            Keyspace.open(KEYSPACE).getColumnFamilyStore(currentTable).cleanupCache();
    }

    public static FunctionName parseFunctionName(String qualifiedName)
    {
        int i = qualifiedName.indexOf('.');
        return i == -1
               ? FunctionName.nativeFunction(qualifiedName)
               : new FunctionName(qualifiedName.substring(0, i).trim(), qualifiedName.substring(i+1).trim());
    }

    public static String shortFunctionName(String f)
    {
        return parseFunctionName(f).name;
    }

    private static void removeAllSSTables(String ks, List<String> tables)
    {
        // clean up data directory which are stored as data directory/keyspace/data files
        for (File d : Directories.getKSChildDirectories(ks))
        {
            if (d.exists() && containsAny(d.getName(), tables))
                FileUtils.deleteRecursive(d);
        }
    }

    private static boolean containsAny(String filename, List<String> tables)
    {
        for (int i = 0, m = tables.size(); i < m; i++)
            // don't accidentally delete in-use directories with the
            // same prefix as a table to delete, i.e. table_1 & table_11
            if (filename.contains(tables.get(i) + "-"))
                return true;
        return false;
    }

    protected String keyspace()
    {
        return KEYSPACE;
    }

    protected String currentTable()
    {
        if (tables.isEmpty())
            return null;
        return tables.get(tables.size() - 1);
    }

    protected ByteBuffer unset()
    {
        return ByteBufferUtil.UNSET_BYTE_BUFFER;
    }

    protected void forcePreparedValues()
    {
        this.usePrepared = true;
    }

    protected void stopForcingPreparedValues()
    {
        this.usePrepared = USE_PREPARED_VALUES;
    }

    protected void disablePreparedReuseForTest()
    {
        this.reusePrepared = false;
    }

    protected String createType(String query)
    {
        String typeName = "type_" + seqNumber.getAndIncrement();
        String fullQuery = String.format(query, KEYSPACE + "." + typeName);
        types.add(typeName);
        logger.info(fullQuery);
        schemaChange(fullQuery);
        return typeName;
    }

    protected String createFunction(String keyspace, String argTypes, String query) throws Throwable
    {
        String functionName = keyspace + ".function_" + seqNumber.getAndIncrement();
        createFunctionOverload(functionName, argTypes, query);
        return functionName;
    }

    protected void createFunctionOverload(String functionName, String argTypes, String query) throws Throwable
    {
        String fullQuery = String.format(query, functionName);
        functions.add(functionName + '(' + argTypes + ')');
        logger.info(fullQuery);
        schemaChange(fullQuery);
    }

    protected String createAggregate(String keyspace, String argTypes, String query) throws Throwable
    {
        String aggregateName = keyspace + "." + "aggregate_" + seqNumber.getAndIncrement();
        createAggregateOverload(aggregateName, argTypes, query);
        return aggregateName;
    }

    protected void createAggregateOverload(String aggregateName, String argTypes, String query) throws Throwable
    {
        String fullQuery = String.format(query, aggregateName);
        aggregates.add(aggregateName + '(' + argTypes + ')');
        logger.info(fullQuery);
        schemaChange(fullQuery);
    }

    protected String createTable(String query)
    {
        String currentTable = createTableName();
        String fullQuery = formatQuery(query);
        logger.info(fullQuery);
        schemaChange(fullQuery);
        return currentTable;
    }

    protected String createTableName()
    {
        String currentTable = "table_" + seqNumber.getAndIncrement();
        tables.add(currentTable);
        return currentTable;
    }

    protected void createTableMayThrow(String query) throws Throwable
    {
        String currentTable = "table_" + seqNumber.getAndIncrement();
        tables.add(currentTable);
        String fullQuery = formatQuery(query);
        logger.info(fullQuery);
        QueryProcessor.executeOnceInternal(fullQuery);
    }

    protected void alterTable(String query)
    {
        String fullQuery = formatQuery(query);
        logger.info(fullQuery);
        schemaChange(fullQuery);
    }

    protected void alterTableMayThrow(String query) throws Throwable
    {
        String fullQuery = formatQuery(query);
        logger.info(fullQuery);
        QueryProcessor.executeOnceInternal(fullQuery);
    }

    protected void dropTable(String query)
    {
        String fullQuery = String.format(query, KEYSPACE + "." + currentTable());
        logger.info(fullQuery);
        schemaChange(fullQuery);
    }

    protected void createIndex(String query)
    {
        String fullQuery = formatQuery(query);
        logger.info(fullQuery);
        schemaChange(fullQuery);
    }

    /**
     * Index creation is asynchronous, this method searches in the system table IndexInfo
     * for the specified index and returns true if it finds it, which indicates the
     * index was built. If we haven't found it after 5 seconds we give-up.
     */
    protected boolean waitForIndex(String keyspace, String table, String index) throws Throwable
    {
        long start = System.currentTimeMillis();
        boolean indexCreated = false;
        while (!indexCreated)
        {
            Object[][] results = getRows(execute("select index_name from system.\"IndexInfo\" where table_name = ?", keyspace));
            for(int i = 0; i < results.length; i++)
            {
                if (index.equals(results[i][0]))
                {
                    indexCreated = true;
                    break;
                }
            }

            if (System.currentTimeMillis() - start > 5000)
                break;

            Thread.sleep(10);
        }

        return indexCreated;
    }

    protected void createIndexMayThrow(String query) throws Throwable
    {
        String fullQuery = formatQuery(query);
        logger.info(fullQuery);
        QueryProcessor.executeOnceInternal(fullQuery);
    }

    protected void dropIndex(String query) throws Throwable
    {
        String fullQuery = String.format(query, KEYSPACE);
        logger.info(fullQuery);
        schemaChange(fullQuery);
    }

    protected void assertLastSchemaChange(Event.SchemaChange.Change change, Event.SchemaChange.Target target,
                                          String keyspace, String name,
                                          String... argTypes)
    {
        Assert.assertTrue(lastSchemaChangeResult instanceof ResultMessage.SchemaChange);
        ResultMessage.SchemaChange schemaChange = (ResultMessage.SchemaChange) lastSchemaChangeResult;
        Assert.assertSame(change, schemaChange.change.change);
        Assert.assertSame(target, schemaChange.change.target);
        Assert.assertEquals(keyspace, schemaChange.change.keyspace);
        Assert.assertEquals(name, schemaChange.change.name);
        Assert.assertEquals(argTypes != null ? Arrays.asList(argTypes) : null, schemaChange.change.argTypes);
    }

    protected static void schemaChange(String query)
    {
        try
        {
            ClientState state = ClientState.forInternalCalls();
            state.setKeyspace(SystemKeyspace.NAME);
            QueryState queryState = new QueryState(state);

            ParsedStatement.Prepared prepared = QueryProcessor.parseStatement(query, queryState);
            prepared.statement.validate(state);

            QueryOptions options = QueryOptions.forInternalCalls(Collections.<ByteBuffer>emptyList());

            lastSchemaChangeResult = prepared.statement.executeInternal(queryState, options);
        }
        catch (Exception e)
        {
            logger.info("Error performing schema change", e);
            throw new RuntimeException("Error setting schema for test (query was: " + query + ")", e);
        }
    }

    protected CFMetaData currentTableMetadata()
    {
        return Schema.instance.getCFMetaData(KEYSPACE, currentTable());
    }

    protected com.datastax.driver.core.ResultSet executeNet(int protocolVersion, String query, Object... values) throws Throwable
    {
        return sessionNet(protocolVersion).execute(formatQuery(query), values);
    }

    protected Session sessionNet()
    {
        return sessionNet(PROTOCOL_VERSIONS.get(PROTOCOL_VERSIONS.size() - 1));
    }

    protected Session sessionNet(int protocolVersion)
    {
        requireNetwork();

        return sessions.get(protocolVersion);
    }

    private String formatQuery(String query)
    {
        String currentTable = currentTable();
        return currentTable == null ? query : String.format(query, KEYSPACE + "." + currentTable);
    }

    protected UntypedResultSet execute(String query, Object... values) throws Throwable
    {
        query = formatQuery(query);

        UntypedResultSet rs;
        if (usePrepared)
        {
            if (logger.isDebugEnabled())
                logger.debug("Executing: {} with values {}", query, formatAllValues(values));
            if (reusePrepared)
            {
                rs = QueryProcessor.executeInternal(query, transformValues(values));

                // If a test uses a "USE ...", then presumably its statements use relative table. In that case, a USE
                // change the meaning of the current keyspace, so we don't want a following statement to reuse a previously
                // prepared statement at this wouldn't use the right keyspace. To avoid that, we drop the previously
                // prepared statement.
                if (query.startsWith("USE"))
                    QueryProcessor.clearInternalStatementsCache();
            }
            else
            {
                rs = QueryProcessor.executeOnceInternal(query, transformValues(values));
            }
        }
        else
        {
            query = replaceValues(query, values);
            if (logger.isDebugEnabled())
                logger.debug("Executing: {}", query);
            rs = QueryProcessor.executeOnceInternal(query);
        }
        if (rs != null)
        {
            if (logger.isDebugEnabled())
                logger.debug("Got {} rows", rs.size());
        }
        return rs;
    }

    protected void assertRowsNet(int protocolVersion, ResultSet result, Object[]... rows)
    {
        // necessary as we need cluster objects to supply CodecRegistry.
        // It's reasonably certain that the network setup has already been done
        // by the time we arrive at this point, but adding this check doesn't hurt
        requireNetwork();

        if (result == null)
        {
            if (rows.length > 0)
                Assert.fail(String.format("No rows returned by query but %d expected", rows.length));
            return;
        }

        ColumnDefinitions meta = result.getColumnDefinitions();
        Iterator<Row> iter = result.iterator();
        int i = 0;
        while (iter.hasNext() && i < rows.length)
        {
            Object[] expected = rows[i];
            Row actual = iter.next();

            Assert.assertEquals(String.format("Invalid number of (expected) values provided for row %d (using protocol version %d)",
                                              i, protocolVersion),
                                meta.size(), expected.length);

            for (int j = 0; j < meta.size(); j++)
            {
                DataType type = meta.getType(j);
                com.datastax.driver.core.TypeCodec<Object> codec = clusters.get(protocolVersion).getConfiguration()
                                                                                                .getCodecRegistry()
                                                                                                .codecFor(type);
                ByteBuffer expectedByteValue = codec.serialize(expected[j], ProtocolVersion.fromInt(protocolVersion));
                int expectedBytes = expectedByteValue == null ? -1 : expectedByteValue.remaining();
                ByteBuffer actualValue = actual.getBytesUnsafe(meta.getName(j));
                int actualBytes = actualValue == null ? -1 : actualValue.remaining();
                if (!Objects.equal(expectedByteValue, actualValue))
                    Assert.fail(String.format("Invalid value for row %d column %d (%s of type %s), " +
                                              "expected <%s> (%d bytes) but got <%s> (%d bytes) " +
                                              "(using protocol version %d)",
                                              i, j, meta.getName(j), type,
                                              codec.format(expected[j]),
                                              expectedBytes,
                                              codec.format(codec.deserialize(actualValue, ProtocolVersion.fromInt(protocolVersion))),
                                              actualBytes,
                                              protocolVersion));
            }
            i++;
        }

        if (iter.hasNext())
        {
            while (iter.hasNext())
            {
                iter.next();
                i++;
            }
            Assert.fail(String.format("Got less rows than expected. Expected %d but got %d (using protocol version %d).",
                                      rows.length, i, protocolVersion));
        }

        Assert.assertTrue(String.format("Got %s rows than expected. Expected %d but got %d (using protocol version %d)",
                                        rows.length>i ? "less" : "more", rows.length, i, protocolVersion), i == rows.length);
    }

    public static void assertRows(UntypedResultSet result, Object[]... rows)
    {
        if (result == null)
        {
            if (rows.length > 0)
                Assert.fail(String.format("No rows returned by query but %d expected", rows.length));
            return;
        }

        List<ColumnSpecification> meta = result.metadata();
        Iterator<UntypedResultSet.Row> iter = result.iterator();
        int i = 0;
        while (iter.hasNext() && i < rows.length)
        {
            Object[] expected = rows[i];
            UntypedResultSet.Row actual = iter.next();

            Assert.assertEquals(String.format("Invalid number of (expected) values provided for row %d", i), expected == null ? 1 : expected.length, meta.size());

            for (int j = 0; j < meta.size(); j++)
            {
                ColumnSpecification column = meta.get(j);
                ByteBuffer expectedByteValue = makeByteBuffer(expected == null ? null : expected[j], column.type);
                ByteBuffer actualValue = actual.getBytes(column.name.toString());

                if (!Objects.equal(expectedByteValue, actualValue))
                {
                    Object actualValueDecoded = actualValue == null ? null : column.type.getSerializer().deserialize(actualValue);
                    if (!Objects.equal(expected[j], actualValueDecoded))
                        Assert.fail(String.format("Invalid value for row %d column %d (%s of type %s), expected <%s> but got <%s>",
                                                  i,
                                                  j,
                                                  column.name,
                                                  column.type.asCQL3Type(),
                                                  formatValue(expectedByteValue, column.type),
                                                  formatValue(actualValue, column.type)));
                }
            }
            i++;
        }

        if (iter.hasNext())
        {
            while (iter.hasNext())
            {
                UntypedResultSet.Row actual = iter.next();
                i++;

                StringBuilder str = new StringBuilder();
                for (int j = 0; j < meta.size(); j++)
                {
                    ColumnSpecification column = meta.get(j);
                    ByteBuffer actualValue = actual.getBytes(column.name.toString());
                    str.append(String.format("%s=%s ", column.name, formatValue(actualValue, column.type)));
                }
                logger.info("Extra row num {}: {}", i, str.toString());
            }
            Assert.fail(String.format("Got more rows than expected. Expected %d but got %d.", rows.length, i));
        }

        Assert.assertTrue(String.format("Got %s rows than expected. Expected %d but got %d", rows.length>i ? "less" : "more", rows.length, i), i == rows.length);
    }

    /**
     * Like assertRows(), but ignores the ordering of rows.
     */
    public static void assertRowsIgnoringOrder(UntypedResultSet result, Object[]... rows)
    {
        if (result == null)
        {
            if (rows.length > 0)
                Assert.fail(String.format("No rows returned by query but %d expected", rows.length));
            return;
        }

        List<ColumnSpecification> meta = result.metadata();

        Set<List<ByteBuffer>> expectedRows = new HashSet<>(rows.length);
        for (Object[] expected : rows)
        {
            Assert.assertEquals("Invalid number of (expected) values provided for row", expected.length, meta.size());
            List<ByteBuffer> expectedRow = new ArrayList<>(meta.size());
            for (int j = 0; j < meta.size(); j++)
                expectedRow.add(makeByteBuffer(expected[j], meta.get(j).type));
            expectedRows.add(expectedRow);
        }

        Set<List<ByteBuffer>> actualRows = new HashSet<>(result.size());
        for (UntypedResultSet.Row actual : result)
        {
            List<ByteBuffer> actualRow = new ArrayList<>(meta.size());
            for (int j = 0; j < meta.size(); j++)
                actualRow.add(actual.getBytes(meta.get(j).name.toString()));
            actualRows.add(actualRow);
        }

        com.google.common.collect.Sets.SetView<List<ByteBuffer>> extra = com.google.common.collect.Sets.difference(actualRows, expectedRows);
        com.google.common.collect.Sets.SetView<List<ByteBuffer>> missing = com.google.common.collect.Sets.difference(expectedRows, actualRows);
        if (!extra.isEmpty() || !missing.isEmpty())
        {
            List<String> extraRows = makeRowStrings(extra, meta);
            List<String> missingRows = makeRowStrings(missing, meta);
            StringBuilder sb = new StringBuilder();
            if (!extra.isEmpty())
            {
                sb.append("Got ").append(extra.size()).append(" extra row(s) ");
                if (!missing.isEmpty())
                    sb.append("and ").append(missing.size()).append(" missing row(s) ");
                sb.append("in result.  Extra rows:\n    ");
                sb.append(extraRows.stream().collect(Collectors.joining("\n    ")));
                if (!missing.isEmpty())
                    sb.append("\nMissing Rows:\n    ").append(missingRows.stream().collect(Collectors.joining("\n    ")));
                Assert.fail(sb.toString());
            }

            if (!missing.isEmpty())
                Assert.fail("Missing " + missing.size() + " row(s) in result: \n    " + missingRows.stream().collect(Collectors.joining("\n    ")));
        }

        assert expectedRows.size() == actualRows.size();
    }

    private static List<String> makeRowStrings(Iterable<List<ByteBuffer>> rows, List<ColumnSpecification> meta)
    {
        List<String> strings = new ArrayList<>();
        for (List<ByteBuffer> row : rows)
        {
            StringBuilder sb = new StringBuilder("row(");
            for (int j = 0; j < row.size(); j++)
            {
                ColumnSpecification column = meta.get(j);
                sb.append(column.name.toString()).append("=").append(formatValue(row.get(j), column.type));
                if (j < (row.size() - 1))
                    sb.append(", ");
            }
            strings.add(sb.append(")").toString());
        }
        return strings;
    }

    protected void assertRowCount(UntypedResultSet result, int numExpectedRows)
    {
        if (result == null)
        {
            if (numExpectedRows > 0)
                Assert.fail(String.format("No rows returned by query but %d expected", numExpectedRows));
            return;
        }

        List<ColumnSpecification> meta = result.metadata();
        Iterator<UntypedResultSet.Row> iter = result.iterator();
        int i = 0;
        while (iter.hasNext() && i < numExpectedRows)
        {
            UntypedResultSet.Row actual = iter.next();
            assertNotNull(actual);
            i++;
        }

        if (iter.hasNext())
        {
            while (iter.hasNext())
            {
                iter.next();
                i++;
            }
            Assert.fail(String.format("Got less rows than expected. Expected %d but got %d.", numExpectedRows, i));
        }

        Assert.assertTrue(String.format("Got %s rows than expected. Expected %d but got %d", numExpectedRows>i ? "less" : "more", numExpectedRows, i), i == numExpectedRows);
    }

    protected Object[][] getRows(UntypedResultSet result)
    {
        if (result == null)
            return new Object[0][];

        List<Object[]> ret = new ArrayList<>();
        List<ColumnSpecification> meta = result.metadata();

        Iterator<UntypedResultSet.Row> iter = result.iterator();
        while (iter.hasNext())
        {
            UntypedResultSet.Row rowVal = iter.next();
            Object[] row = new Object[meta.size()];
            for (int j = 0; j < meta.size(); j++)
            {
                ColumnSpecification column = meta.get(j);
                ByteBuffer val = rowVal.getBytes(column.name.toString());
                row[j] = val == null ? null : column.type.getSerializer().deserialize(val);
            }

            ret.add(row);
        }

        Object[][] a = new Object[ret.size()][];
        return ret.toArray(a);
    }

    protected void assertColumnNames(UntypedResultSet result, String... expectedColumnNames)
    {
        if (result == null)
        {
            Assert.fail("No rows returned by query.");
            return;
        }

        List<ColumnSpecification> metadata = result.metadata();
        Assert.assertEquals("Got less columns than expected.", expectedColumnNames.length, metadata.size());

        for (int i = 0, m = metadata.size(); i < m; i++)
        {
            ColumnSpecification columnSpec = metadata.get(i);
            Assert.assertEquals(expectedColumnNames[i], columnSpec.name.toString());
        }
    }

    protected void assertAllRows(Object[]... rows) throws Throwable
    {
        assertRows(execute("SELECT * FROM %s"), rows);
    }

    public static Object[] row(Object... expected)
    {
        return expected;
    }

    protected void assertEmpty(UntypedResultSet result) throws Throwable
    {
        if (result != null && !result.isEmpty())
            throw new AssertionError(String.format("Expected empty result but got %d rows", result.size()));
    }

    protected void assertInvalid(String query, Object... values) throws Throwable
    {
        assertInvalidMessage(null, query, values);
    }

    protected void assertInvalidMessage(String errorMessage, String query, Object... values) throws Throwable
    {
        assertInvalidThrowMessage(errorMessage, null, query, values);
    }

    protected void assertInvalidThrow(Class<? extends Throwable> exception, String query, Object... values) throws Throwable
    {
        assertInvalidThrowMessage(null, exception, query, values);
    }

    protected void assertInvalidThrowMessage(String errorMessage, Class<? extends Throwable> exception, String query, Object... values) throws Throwable
    {
        assertInvalidThrowMessage(Integer.MIN_VALUE, errorMessage, exception, query, values);
    }

    // if a protocol version > Integer.MIN_VALUE is supplied, executes
    // the query via the java driver, mimicking a real client.
    protected void assertInvalidThrowMessage(int protocolVersion,
                                             String errorMessage,
                                             Class<? extends Throwable> exception,
                                             String query,
                                             Object... values) throws Throwable
    {
        try
        {
            if (protocolVersion == Integer.MIN_VALUE)
                execute(query, values);
            else
                executeNet(protocolVersion, query, values);

            String q = USE_PREPARED_VALUES
                       ? query + " (values: " + formatAllValues(values) + ")"
                       : replaceValues(query, values);
            Assert.fail("Query should be invalid but no error was thrown. Query is: " + q);
        }
        catch (Exception e)
        {
            if (exception != null && !exception.isAssignableFrom(e.getClass()))
            {
                Assert.fail("Query should be invalid but wrong error was thrown. " +
                            "Expected: " + exception.getName() + ", got: " + e.getClass().getName() + ". " +
                            "Query is: " + queryInfo(query, values));
            }
            if (errorMessage != null)
            {
                assertMessageContains(errorMessage, e);
            }
        }
    }

    private static String queryInfo(String query, Object[] values)
    {
        return USE_PREPARED_VALUES
               ? query + " (values: " + formatAllValues(values) + ")"
               : replaceValues(query, values);
    }

    protected void assertValidSyntax(String query) throws Throwable
    {
        try
        {
            QueryProcessor.parseStatement(query);
        }
        catch(SyntaxException e)
        {
            Assert.fail(String.format("Expected query syntax to be valid but was invalid. Query is: %s; Error is %s",
                                      query, e.getMessage()));
        }
    }

    protected void assertInvalidSyntax(String query, Object... values) throws Throwable
    {
        assertInvalidSyntaxMessage(null, query, values);
    }

    protected void assertInvalidSyntaxMessage(String errorMessage, String query, Object... values) throws Throwable
    {
        try
        {
            execute(query, values);
            Assert.fail("Query should have invalid syntax but no error was thrown. Query is: " + queryInfo(query, values));
        }
        catch (SyntaxException e)
        {
            if (errorMessage != null)
            {
                assertMessageContains(errorMessage, e);
            }
        }
    }

    /**
     * Asserts that the message of the specified exception contains the specified text.
     *
     * @param text the text that the exception message must contains
     * @param e the exception to check
     */
    private static void assertMessageContains(String text, Exception e)
    {
        Assert.assertTrue("Expected error message to contain '" + text + "', but got '" + e.getMessage() + "'",
                e.getMessage().contains(text));
    }

    @FunctionalInterface
    public interface CheckedFunction {
        void apply() throws Throwable;
    }

    /**
     * Runs the given function before and after a flush of sstables.  This is useful for checking that behavior is
     * the same whether data is in memtables or sstables.
     * @param runnable
     * @throws Throwable
     */
    public void beforeAndAfterFlush(CheckedFunction runnable) throws Throwable
    {
        runnable.apply();
        flush();
        runnable.apply();
    }

    private static String replaceValues(String query, Object[] values)
    {
        StringBuilder sb = new StringBuilder();
        int last = 0;
        int i = 0;
        int idx;
        while ((idx = query.indexOf('?', last)) > 0)
        {
            if (i >= values.length)
                throw new IllegalArgumentException(String.format("Not enough values provided. The query has at least %d variables but only %d values provided", i, values.length));

            sb.append(query.substring(last, idx));

            Object value = values[i++];

            // When we have a .. IN ? .., we use a list for the value because that's what's expected when the value is serialized.
            // When we format as string however, we need to special case to use parenthesis. Hackish but convenient.
            if (idx >= 3 && value instanceof List && query.substring(idx - 3, idx).equalsIgnoreCase("IN "))
            {
                List l = (List)value;
                sb.append("(");
                for (int j = 0; j < l.size(); j++)
                {
                    if (j > 0)
                        sb.append(", ");
                    sb.append(formatForCQL(l.get(j)));
                }
                sb.append(")");
            }
            else
            {
                sb.append(formatForCQL(value));
            }
            last = idx + 1;
        }
        sb.append(query.substring(last));
        return sb.toString();
    }

    // We're rellly only returning ByteBuffers but this make the type system happy
    private static Object[] transformValues(Object[] values)
    {
        // We could partly rely on QueryProcessor.executeOnceInternal doing type conversion for us, but
        // it would complain with ClassCastException if we pass say a string where an int is excepted (since
        // it bases conversion on what the value should be, not what it is). For testing, we sometimes
        // want to pass value of the wrong type and assert that this properly raise an InvalidRequestException
        // and executeOnceInternal goes into way. So instead, we pre-convert everything to bytes here based
        // on the value.
        // Besides, we need to handle things like TupleValue that executeOnceInternal don't know about.

        Object[] buffers = new ByteBuffer[values.length];
        for (int i = 0; i < values.length; i++)
        {
            Object value = values[i];
            if (value == null)
            {
                buffers[i] = null;
                continue;
            }
            else if (value == ByteBufferUtil.UNSET_BYTE_BUFFER)
            {
                buffers[i] = ByteBufferUtil.UNSET_BYTE_BUFFER;
                continue;
            }

            try
            {
                buffers[i] = typeFor(value).decompose(serializeTuples(value));
            }
            catch (Exception ex)
            {
                logger.info("Error serializing query parameter {}:", value, ex);
                throw ex;
            }
        }
        return buffers;
    }

    private static Object serializeTuples(Object value)
    {
        if (value instanceof TupleValue)
        {
            return ((TupleValue)value).toByteBuffer();
        }

        // We need to reach inside collections for TupleValue and transform them to ByteBuffer
        // since otherwise the decompose method of the collection AbstractType won't know what
        // to do with them
        if (value instanceof List)
        {
            List l = (List)value;
            List n = new ArrayList(l.size());
            for (Object o : l)
                n.add(serializeTuples(o));
            return n;
        }

        if (value instanceof Set)
        {
            Set s = (Set)value;
            Set n = new LinkedHashSet(s.size());
            for (Object o : s)
                n.add(serializeTuples(o));
            return n;
        }

        if (value instanceof Map)
        {
            Map m = (Map)value;
            Map n = new LinkedHashMap(m.size());
            for (Object entry : m.entrySet())
                n.put(serializeTuples(((Map.Entry)entry).getKey()), serializeTuples(((Map.Entry)entry).getValue()));
            return n;
        }
        return value;
    }

    private static String formatAllValues(Object[] values)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (int i = 0; i < values.length; i++)
        {
            if (i > 0)
                sb.append(", ");
            sb.append(formatForCQL(values[i]));
        }
        sb.append("]");
        return sb.toString();
    }

    private static String formatForCQL(Object value)
    {
        if (value == null)
            return "null";

        if (value instanceof TupleValue)
            return ((TupleValue)value).toCQLString();

        // We need to reach inside collections for TupleValue. Besides, for some reason the format
        // of collection that CollectionType.getString gives us is not at all 'CQL compatible'
        if (value instanceof Collection || value instanceof Map)
        {
            StringBuilder sb = new StringBuilder();
            if (value instanceof List)
            {
                List l = (List)value;
                sb.append("[");
                for (int i = 0; i < l.size(); i++)
                {
                    if (i > 0)
                        sb.append(", ");
                    sb.append(formatForCQL(l.get(i)));
                }
                sb.append("]");
            }
            else if (value instanceof Set)
            {
                Set s = (Set)value;
                sb.append("{");
                Iterator iter = s.iterator();
                while (iter.hasNext())
                {
                    sb.append(formatForCQL(iter.next()));
                    if (iter.hasNext())
                        sb.append(", ");
                }
                sb.append("}");
            }
            else
            {
                Map m = (Map)value;
                sb.append("{");
                Iterator iter = m.entrySet().iterator();
                while (iter.hasNext())
                {
                    Map.Entry entry = (Map.Entry)iter.next();
                    sb.append(formatForCQL(entry.getKey())).append(": ").append(formatForCQL(entry.getValue()));
                    if (iter.hasNext())
                        sb.append(", ");
                }
                sb.append("}");
            }
            return sb.toString();
        }

        AbstractType type = typeFor(value);
        String s = type.getString(type.decompose(value));

        if (type instanceof InetAddressType || type instanceof TimestampType)
            return String.format("'%s'", s);
        else if (type instanceof UTF8Type)
            return String.format("'%s'", s.replaceAll("'", "''"));
        else if (type instanceof BytesType)
            return "0x" + s;

        return s;
    }

    private static ByteBuffer makeByteBuffer(Object value, AbstractType type)
    {
        if (value == null)
            return null;

        if (value instanceof TupleValue)
            return ((TupleValue)value).toByteBuffer();

        if (value instanceof ByteBuffer)
            return (ByteBuffer)value;

        return type.decompose(serializeTuples(value));
    }

    private static String formatValue(ByteBuffer bb, AbstractType<?> type)
    {
        if (bb == null)
            return "null";

        if (type instanceof CollectionType)
        {
            // CollectionType override getString() to use hexToBytes. We can't change that
            // without breaking SSTable2json, but the serializer for collection have the
            // right getString so using it directly instead.
            TypeSerializer ser = type.getSerializer();
            return ser.toString(ser.deserialize(bb));
        }

        return type.getString(bb);
    }

    protected Object tuple(Object...values)
    {
        return new TupleValue(values);
    }

    protected Object userType(Object... values)
    {
        if (values.length % 2 != 0)
            throw new IllegalArgumentException("userType() requires an even number of arguments");

        String[] fieldNames = new String[values.length / 2];
        Object[] fieldValues = new Object[values.length / 2];
        int fieldNum = 0;
        for (int i = 0; i < values.length; i += 2)
        {
            fieldNames[fieldNum] = (String) values[i];
            fieldValues[fieldNum] = values[i + 1];
            fieldNum++;
        }
        return new UserTypeValue(fieldNames, fieldValues);
    }

    protected Object list(Object...values)
    {
        return Arrays.asList(values);
    }

    protected Object set(Object...values)
    {
        return ImmutableSet.copyOf(values);
    }

    protected Object map(Object...values)
    {
        if (values.length % 2 != 0)
            throw new IllegalArgumentException();

        int size = values.length / 2;
        Map m = new LinkedHashMap(size);
        for (int i = 0; i < size; i++)
            m.put(values[2 * i], values[(2 * i) + 1]);
        return m;
    }

    protected com.datastax.driver.core.TupleType tupleTypeOf(int protocolVersion, DataType...types)
    {
        requireNetwork();
        return clusters.get(protocolVersion).getMetadata().newTupleType(types);
    }

    // Attempt to find an AbstracType from a value (for serialization/printing sake).
    // Will work as long as we use types we know of, which is good enough for testing
    private static AbstractType typeFor(Object value)
    {
        if (value instanceof ByteBuffer || value instanceof TupleValue || value == null)
            return BytesType.instance;

        if (value instanceof Byte)
            return ByteType.instance;

        if (value instanceof Short)
            return ShortType.instance;

        if (value instanceof Integer)
            return Int32Type.instance;

        if (value instanceof Long)
            return LongType.instance;

        if (value instanceof Float)
            return FloatType.instance;

        if (value instanceof Double)
            return DoubleType.instance;

        if (value instanceof BigInteger)
            return IntegerType.instance;

        if (value instanceof BigDecimal)
            return DecimalType.instance;

        if (value instanceof String)
            return UTF8Type.instance;

        if (value instanceof Boolean)
            return BooleanType.instance;

        if (value instanceof InetAddress)
            return InetAddressType.instance;

        if (value instanceof Date)
            return TimestampType.instance;

        if (value instanceof UUID)
            return UUIDType.instance;

        if (value instanceof List)
        {
            List l = (List)value;
            AbstractType elt = l.isEmpty() ? BytesType.instance : typeFor(l.get(0));
            return ListType.getInstance(elt, true);
        }

        if (value instanceof Set)
        {
            Set s = (Set)value;
            AbstractType elt = s.isEmpty() ? BytesType.instance : typeFor(s.iterator().next());
            return SetType.getInstance(elt, true);
        }

        if (value instanceof Map)
        {
            Map m = (Map)value;
            AbstractType keys, values;
            if (m.isEmpty())
            {
                keys = BytesType.instance;
                values = BytesType.instance;
            }
            else
            {
                Map.Entry entry = (Map.Entry)m.entrySet().iterator().next();
                keys = typeFor(entry.getKey());
                values = typeFor(entry.getValue());
            }
            return MapType.getInstance(keys, values, true);
        }

        throw new IllegalArgumentException("Unsupported value type (value is " + value + ")");
    }

    private static class TupleValue
    {
        protected final Object[] values;

        TupleValue(Object[] values)
        {
            this.values = values;
        }

        public ByteBuffer toByteBuffer()
        {
            ByteBuffer[] bbs = new ByteBuffer[values.length];
            for (int i = 0; i < values.length; i++)
                bbs[i] = makeByteBuffer(values[i], typeFor(values[i]));
            return TupleType.buildValue(bbs);
        }

        public String toCQLString()
        {
            StringBuilder sb = new StringBuilder();
            sb.append("(");
            for (int i = 0; i < values.length; i++)
            {
                if (i > 0)
                    sb.append(", ");
                sb.append(formatForCQL(values[i]));
            }
            sb.append(")");
            return sb.toString();
        }

        public String toString()
        {
            return "TupleValue" + toCQLString();
        }
    }

    private static class UserTypeValue extends TupleValue
    {
        private final String[] fieldNames;

        UserTypeValue(String[] fieldNames, Object[] fieldValues)
        {
            super(fieldValues);
            this.fieldNames = fieldNames;
        }

        @Override
        public String toCQLString()
        {
            StringBuilder sb = new StringBuilder();
            sb.append("{");
            boolean haveEntry = false;
            for (int i = 0; i < values.length; i++)
            {
                if (values[i] != null)
                {
                    if (haveEntry)
                        sb.append(", ");
                    sb.append(ColumnIdentifier.maybeQuote(fieldNames[i]));
                    sb.append(": ");
                    sb.append(formatForCQL(values[i]));
                    haveEntry = true;
                }
            }
            assert haveEntry;
            sb.append("}");
            return sb.toString();
        }

        public String toString()
        {
            return "UserTypeValue" + toCQLString();
        }
    }
}
