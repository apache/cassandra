package org.apache.cassandra.service.epaxos;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import org.apache.cassandra.config.*;
import org.apache.cassandra.cql3.Attributes;
import org.apache.cassandra.cql3.BatchQueryOptions;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.CQL3CasRequest;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.db.ArrayBackedSortedColumns;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.composites.CellNames;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.CASRequest;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.ThriftCASRequest;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.*;

public class EpaxosSerializationTest
{
    private static KSMetaData ksm;
    private static CFMetaData cqlcf;
    private static CFMetaData thriftcf;
    protected static final InetAddress LOCALHOST;
    private static int VERSION = MessagingService.current_version;

    static
    {
        DatabaseDescriptor.getConcurrentWriters();
        MessagingService.instance();
        try
        {
            LOCALHOST = InetAddress.getByName("127.0.0.1");
        }
        catch (UnknownHostException e)
        {
            throw new AssertionError(e);
        }
    }

    @BeforeClass
    public static void setUpClass() throws Exception
    {
        cqlcf = CFMetaData.compile("CREATE TABLE ks.tbl (k INT PRIMARY KEY, v INT);", "ks");
        Map<String, String> ksOpts = new HashMap<>();
        ksOpts.put("replication_factor", "1");
        thriftcf = CFMetaData.denseCFMetaData("ks", "thrifttbl", Int32Type.instance);
        ksm = KSMetaData.newKeyspace("ks", SimpleStrategy.class, ksOpts, true, Arrays.asList(cqlcf, thriftcf));
        Schema.instance.load(ksm);
    }

    protected ThriftCASRequest getThriftCasRequest()
    {
        ColumnFamily expected = ArrayBackedSortedColumns.factory.create("ks", "thrifttbl");
        expected.addColumn(CellNames.simpleDense(ByteBufferUtil.bytes("v")), ByteBufferUtil.bytes(2), 3L);

        ColumnFamily updates = ArrayBackedSortedColumns.factory.create("ks", "thrifttbl");
        updates.addColumn(CellNames.simpleDense(ByteBufferUtil.bytes("v")), ByteBufferUtil.bytes(5), 6L);

        return new ThriftCASRequest(expected, updates);
    }

    protected CQL3CasRequest getCqlCasRequest(int k, int v)
    {
        return getCqlCasRequest(k, v, true);
    }

    protected CQL3CasRequest getCqlCasRequest(int k, int v, boolean withOpts)
    {
        try
        {
            String query = withOpts ? "INSERT INTO ks.tbl (k, v) VALUES (?, ?) IF NOT EXISTS"
                                    : String.format("INSERT INTO ks.tbl (k, v) VALUES (%s, %s) IF NOT EXISTS", k, v);
            ModificationStatement.Parsed parsed = (ModificationStatement.Parsed) QueryProcessor.parseStatement(query);
            parsed.prepareKeyspace("ks");
            parsed.setQueryString(query);
            ParsedStatement.Prepared prepared = parsed.prepare();

            List<ByteBuffer> values = withOpts ? Lists.newArrayList(ByteBufferUtil.bytes(k), ByteBufferUtil.bytes(v))
                                               : Lists.<ByteBuffer>newArrayList();

            QueryOptions options = QueryOptions.create(ConsistencyLevel.SERIAL,
                                                       values,
                                                       false, 1, null, ConsistencyLevel.QUORUM);
            options.prepare(prepared.boundNames);
            QueryState state = QueryState.forInternalCalls();

            ModificationStatement statement = (ModificationStatement) prepared.statement;

            return statement.createCasRequest(state, options);
        }
        catch (Exception e)
        {
            throw new AssertionError(e);
        }
    }

    protected SerializedRequest newSerializedRequest(CASRequest request)
    {
        SerializedRequest.Builder builder = SerializedRequest.builder();
        builder.casRequest(request);
        builder.cfName(cqlcf.cfName);
        builder.keyspaceName(cqlcf.ksName);
        builder.key(ByteBufferUtil.bytes(7));
        builder.consistencyLevel(ConsistencyLevel.SERIAL);
        return builder.build();
    }

    protected SerializedRequest getSerializedThriftRequest()
    {
        ThriftCASRequest casRequest = getThriftCasRequest();
        return newSerializedRequest(casRequest);
    }

    protected SerializedRequest getSerializedCQLRequest(int k, int v)
    {
        CQL3CasRequest casRequest = getCqlCasRequest(k, v);
        return newSerializedRequest(casRequest);
    }

    @Test
    public void checkThriftCasRequest() throws Exception
    {
        ThriftCASRequest request = getThriftCasRequest();

        DataOutputBuffer out = new DataOutputBuffer();
        ThriftCASRequest.serializer.serialize(request, out, VERSION);
        int expectedSize = out.getLength();
        Assert.assertEquals(expectedSize, ThriftCASRequest.serializer.serializedSize(request, VERSION));

        CASRequest deserialized = ThriftCASRequest.serializer.deserialize(ByteStreams.newDataInput(out.getData()), VERSION);

        Assert.assertEquals(request, deserialized);
    }

    @Test
    public void checkCqlCasRequest() throws Exception
    {
        CQL3CasRequest request = getCqlCasRequest(1, 2);

        DataOutputBuffer out = new DataOutputBuffer();
        CASRequest.serializer.serialize(request, out, VERSION);
        int expectedSize = out.getLength();
        Assert.assertEquals(expectedSize, CASRequest.serializer.serializedSize(request, VERSION));

        CASRequest deserialized = CASRequest.serializer.deserialize(ByteStreams.newDataInput(out.getData()), VERSION);

//        Assert.assertEquals(request, deserialized);
    }

    @Test
    public void checkCqlCasRequestNoOpts() throws Exception
    {
        CQL3CasRequest request = getCqlCasRequest(1, 2, false);

        DataOutputBuffer out = new DataOutputBuffer();
        CASRequest.serializer.serialize(request, out, VERSION);
        int expectedSize = out.getLength();
        Assert.assertEquals(expectedSize, CASRequest.serializer.serializedSize(request, VERSION));

        CASRequest deserialized = CASRequest.serializer.deserialize(ByteStreams.newDataInput(out.getData()), VERSION);

//        Assert.assertEquals(request, deserialized);
    }

    @Test
    public void batchQueryCasRequest() throws Exception
    {
        String q = "BEGIN BATCH\n" +
                   "INSERT INTO ks.tbl (k, v) VALUES (0, 0);\n" +
                   "UPDATE ks.tbl SET v = ? WHERE k = 0 IF v = ?;\n" +
                   "UPDATE ks.tbl SET v = ? WHERE k = 0 IF v = ?;\n" +
                   "APPLY BATCH";
        BatchStatement.Parsed parsed = (BatchStatement.Parsed) QueryProcessor.parseStatement(q);
        parsed.setQueryString(q);

        ParsedStatement.Prepared prepared = parsed.prepare();
        List<ByteBuffer> values = Lists.newArrayList(ByteBufferUtil.bytes(1), ByteBufferUtil.bytes(0),
                                                     ByteBufferUtil.bytes(2), ByteBufferUtil.bytes(1));

        QueryOptions options = QueryOptions.create(ConsistencyLevel.SERIAL,
                                                   values,
                                                   false, 1, null, ConsistencyLevel.QUORUM);
        options.prepare(prepared.boundNames);
        BatchStatement statement = (BatchStatement) prepared.statement;
        BatchQueryOptions batchOptions = BatchQueryOptions.withoutPerStatementVariables(options);
        long now = System.currentTimeMillis();

        CQL3CasRequest request = statement.getCasRequest(batchOptions, now);

        DataOutputBuffer out = new DataOutputBuffer();
        CASRequest.serializer.serialize(request, out, VERSION);
        int expectedSize = out.getLength();
        Assert.assertEquals(expectedSize, CASRequest.serializer.serializedSize(request, VERSION));

        CASRequest deserialized = CASRequest.serializer.deserialize(ByteStreams.newDataInput(out.getData()), VERSION);
    }

    @Test
    public void batchQueryCasRequestNoOpts() throws Exception
    {
        String q = "BEGIN BATCH\n" +
                   "INSERT INTO ks.tbl (k, v) VALUES (0, 0);\n" +
                   "UPDATE ks.tbl SET v = 1 WHERE k = 0 IF v = 0;\n" +
                   "UPDATE ks.tbl SET v = 2 WHERE k = 0 IF v = 1;\n" +
                   "APPLY BATCH";
        BatchStatement.Parsed parsed = (BatchStatement.Parsed) QueryProcessor.parseStatement(q);
        parsed.setQueryString(q);

        ParsedStatement.Prepared prepared = parsed.prepare();
        List<ByteBuffer> values = Lists.newArrayList();

        QueryOptions options = QueryOptions.create(ConsistencyLevel.SERIAL,
                                                   values,
                                                   false, 1, null, ConsistencyLevel.QUORUM);
        options.prepare(prepared.boundNames);
        BatchStatement statement = (BatchStatement) prepared.statement;
        BatchQueryOptions batchOptions = BatchQueryOptions.withoutPerStatementVariables(options);
        long now = System.currentTimeMillis();

        CQL3CasRequest request = statement.getCasRequest(batchOptions, now);

        DataOutputBuffer out = new DataOutputBuffer();
        CASRequest.serializer.serialize(request, out, VERSION);
        int expectedSize = out.getLength();
        Assert.assertEquals(expectedSize, CASRequest.serializer.serializedSize(request, VERSION));

        CASRequest deserialized = CASRequest.serializer.deserialize(ByteStreams.newDataInput(out.getData()), VERSION);
    }

    private ModificationStatement getStatement(String ks, String query) throws Exception
    {
        ModificationStatement.Parsed parsed = (ModificationStatement.Parsed) QueryProcessor.parseStatement(query);
        parsed.prepareKeyspace(ks);
        parsed.setQueryString(query);
        ParsedStatement.Prepared prepared = parsed.prepare();

        return (ModificationStatement) prepared.statement;
    }

    private static ByteBuffer bb(int b)
    {
        return ByteBufferUtil.bytes(b);
    }

    @Test
    public void batchMessageCasRequest() throws Exception
    {
        List<ModificationStatement> statements = new ArrayList<>(3);
        List<List<ByteBuffer>> variables = new ArrayList<>(3);

        statements.add(getStatement("ks", "INSERT INTO ks.tbl (k, v) VALUES (?, ?);"));
        variables.add(Lists.newArrayList(bb(0), bb(0)));

        statements.add(getStatement("ks", "UPDATE ks.tbl SET v = ? WHERE k = ? IF v = ?;"));
        variables.add(Lists.newArrayList(bb(1), bb(0), bb(0)));

        statements.add(getStatement("ks", "UPDATE ks.tbl SET v = ? WHERE k = ? IF v = ?;"));
        variables.add(Lists.newArrayList(bb(2), bb(0), bb(1)));

        QueryOptions options = QueryOptions.create(ConsistencyLevel.SERIAL,
                                                   Lists.<ByteBuffer>newArrayList(),
                                                   false, 1, null, ConsistencyLevel.QUORUM);

        List<Object> queryOrIdList = new ArrayList<>(3);
        for (ModificationStatement statement: statements)
        {
            queryOrIdList.add(statement.getQueryString());
        }

        BatchQueryOptions batchOptions = BatchQueryOptions.withPerStatementVariables(options, variables, queryOrIdList);
        BatchStatement statement = new BatchStatement(-1, BatchStatement.Type.LOGGED, statements, Attributes.none());

        CQL3CasRequest request = statement.getCasRequest(batchOptions, System.currentTimeMillis());
        DataOutputBuffer out = new DataOutputBuffer();
        CASRequest.serializer.serialize(request, out, VERSION);
        int expectedSize = out.getLength();
        Assert.assertEquals(expectedSize, CASRequest.serializer.serializedSize(request, VERSION));

        CASRequest deserialized = CASRequest.serializer.deserialize(ByteStreams.newDataInput(out.getData()), VERSION);
    }

    @Test
    public void batchMessageCasRequestNoOpts() throws Exception
    {
        List<ByteBuffer> NONE = ImmutableList.of();

        List<ModificationStatement> statements = new ArrayList<>(3);
        List<List<ByteBuffer>> variables = new ArrayList<>(3);

        statements.add(getStatement("ks", "INSERT INTO ks.tbl (k, v) VALUES (0, 0);"));
        variables.add(NONE);

        statements.add(getStatement("ks", "UPDATE ks.tbl SET v = 1 WHERE k = 0 IF v = 0;"));
        variables.add(NONE);

        statements.add(getStatement("ks", "UPDATE ks.tbl SET v = 2 WHERE k = 0 IF v = 1;"));
        variables.add(NONE);

        QueryOptions options = QueryOptions.create(ConsistencyLevel.SERIAL,
                                                   Lists.<ByteBuffer>newArrayList(),
                                                   false, 1, null, ConsistencyLevel.QUORUM);

        List<Object> queryOrIdList = new ArrayList<>(3);
        for (ModificationStatement statement: statements)
        {
            queryOrIdList.add(statement.getQueryString());
        }

        BatchQueryOptions batchOptions = BatchQueryOptions.withPerStatementVariables(options, variables, queryOrIdList);
        BatchStatement statement = new BatchStatement(-1, BatchStatement.Type.LOGGED, statements, Attributes.none());

        CQL3CasRequest request = statement.getCasRequest(batchOptions, System.currentTimeMillis());
        DataOutputBuffer out = new DataOutputBuffer();
        CASRequest.serializer.serialize(request, out, VERSION);
        int expectedSize = out.getLength();
        Assert.assertEquals(expectedSize, CASRequest.serializer.serializedSize(request, VERSION));

        CASRequest deserialized = CASRequest.serializer.deserialize(ByteStreams.newDataInput(out.getData()), VERSION);
    }

    @Test
    public void serializedReadRequest() throws IOException
    {
        ReadCommand readCommand = ReadCommand.create(cqlcf.ksName, ByteBufferUtil.bytes(1), cqlcf.cfName, 100,
                                                     new SliceQueryFilter(ColumnSlice.ALL_COLUMNS, false, 10));

        SerializedRequest.Builder builder = SerializedRequest.builder();
        builder.readCommand(readCommand);
        builder.cfName(readCommand.cfName);
        builder.keyspaceName(readCommand.ksName);
        builder.key(readCommand.key);
        builder.consistencyLevel(ConsistencyLevel.SERIAL);
        SerializedRequest request = builder.build();

        DataOutputBuffer out = new DataOutputBuffer();
        SerializedRequest.serializer.serialize(request, out, VERSION);
        int expectedSize = out.getLength();
        Assert.assertEquals(expectedSize, SerializedRequest.serializer.serializedSize(request, VERSION));

        SerializedRequest deserialized = SerializedRequest.serializer.deserialize(ByteStreams.newDataInput(out.getData()), VERSION);

        Assert.assertEquals(request, deserialized);
    }

    private SerializedRequest getSerializedRequest()
    {
        ThriftCASRequest thriftRequest = getThriftCasRequest();
        SerializedRequest.Builder builder = SerializedRequest.builder();
        builder.casRequest(thriftRequest);
        builder.cfName(cqlcf.cfName);
        builder.keyspaceName(cqlcf.ksName);
        builder.key(ByteBufferUtil.bytes(7));
        builder.consistencyLevel(ConsistencyLevel.SERIAL);
        return builder.build();
    }

    @Test
    public void checkSerializedRequest() throws Exception
    {
        SerializedRequest request = getSerializedRequest();

        DataOutputBuffer out = new DataOutputBuffer();
        SerializedRequest.serializer.serialize(request, out, VERSION);
        int expectedSize = out.getLength();
        Assert.assertEquals(expectedSize, SerializedRequest.serializer.serializedSize(request, VERSION));

        SerializedRequest deserialized = SerializedRequest.serializer.deserialize(ByteStreams.newDataInput(out.getData()), VERSION);

        Assert.assertEquals(request, deserialized);
    }

    @Test
    public void checkInstance() throws Exception
    {
        Instance instance = new QueryInstance(getSerializedRequest(), LOCALHOST);
        Set<UUID> deps = Sets.newHashSet(UUIDGen.getTimeUUID(), UUIDGen.getTimeUUID());
        instance.preaccept(deps, deps);
        instance.updateBallot(5);

        // shouldn't be serialized
        instance.setStronglyConnected(Sets.newHashSet(UUIDGen.getTimeUUID(), UUIDGen.getTimeUUID()));

        DataOutputBuffer out = new DataOutputBuffer();
        Instance.serializer.serialize(instance, out, VERSION);
        int expectedSize = out.getLength();
        Assert.assertEquals(expectedSize, Instance.serializer.serializedSize(instance, VERSION));

        Instance deserialized = Instance.serializer.deserialize(ByteStreams.newDataInput(out.getData()), VERSION);

        Assert.assertEquals(instance.getId(), deserialized.getId());
        Assert.assertEquals(instance.getState(), deserialized.getState());
        Assert.assertEquals(instance.getDependencies(), deserialized.getDependencies());
        Assert.assertEquals(instance.getLeaderAttrsMatch(), deserialized.getLeaderAttrsMatch());
        Assert.assertEquals(instance.getBallot(), deserialized.getBallot());

        // check unserialized attributes
        Assert.assertNotNull(instance.getStronglyConnected());
        Assert.assertNull(deserialized.getStronglyConnected());
    }

    @Test
    public void checkInstanceInternal() throws Exception
    {
        Instance instance = new QueryInstance(getSerializedRequest(), LOCALHOST);
        Set<UUID> deps = Sets.newHashSet(UUIDGen.getTimeUUID(), UUIDGen.getTimeUUID());
        instance.preaccept(deps, deps);
        instance.updateBallot(5);

        // shouldn't be serialized
        instance.setStronglyConnected(Sets.newHashSet(UUIDGen.getTimeUUID(), UUIDGen.getTimeUUID()));
        instance.setPlaceholder(true);

        DataOutputBuffer out = new DataOutputBuffer();
        Instance.internalSerializer.serialize(instance, out, VERSION);
        int expectedSize = out.getLength();
        Assert.assertEquals(expectedSize, Instance.internalSerializer.serializedSize(instance, VERSION));

        Instance deserialized = Instance.internalSerializer.deserialize(ByteStreams.newDataInput(out.getData()), VERSION);

        Assert.assertEquals(instance.getId(), deserialized.getId());
        Assert.assertEquals(instance.getState(), deserialized.getState());
        Assert.assertEquals(instance.getDependencies(), deserialized.getDependencies());
        Assert.assertEquals(instance.getLeaderAttrsMatch(), deserialized.getLeaderAttrsMatch());
        Assert.assertEquals(instance.getBallot(), deserialized.getBallot());

        // check unserialized attributes
        Assert.assertEquals(instance.getStronglyConnected(), deserialized.getStronglyConnected());
        Assert.assertEquals(instance.isPlaceholder(), deserialized.isPlaceholder());
    }

    @Test
    public void checkNullInstance() throws Exception
    {
        Instance instance = null;

        DataOutputBuffer out = new DataOutputBuffer();
        Instance.serializer.serialize(instance, out, VERSION);
        int expectedSize = out.getLength();
        Assert.assertEquals(expectedSize, Instance.serializer.serializedSize(instance, VERSION));

        Instance deserialized = Instance.serializer.deserialize(ByteStreams.newDataInput(out.getData()), VERSION);

        Assert.assertNull(deserialized);
    }
}
