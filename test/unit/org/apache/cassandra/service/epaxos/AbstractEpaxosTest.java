package org.apache.cassandra.service.epaxos;

import com.google.common.collect.Lists;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.CQL3CasRequest;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.db.ArrayBackedSortedColumns;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.composites.CellNames;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.CASRequest;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.ThriftCASRequest;
import org.apache.cassandra.utils.ByteBufferUtil;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Ignore
public abstract class AbstractEpaxosTest
{
    protected static final Logger logger = LoggerFactory.getLogger(AbstractEpaxosTest.class);
    protected static KSMetaData ksm;
    protected static CFMetaData cfm;
    protected static CFMetaData thriftcf;
    protected static final InetAddress LOCALHOST;
    protected static InetAddress LOCAL_ADDRESS;
    protected static InetAddress REMOTE_ADDRESS;
    protected static final Scope DEFAULT_SCOPE = Scope.GLOBAL;
    protected static final String DC1 = "DC1";
    protected static final String DC2 = "DC2";

    static
    {
        DatabaseDescriptor.setPartitioner(new ByteOrderedPartitioner());
        DatabaseDescriptor.getConcurrentWriters();
        MessagingService.instance();
        SchemaLoader.prepareServer();
        try
        {
            LOCALHOST = InetAddress.getByName("127.0.0.1");
            LOCAL_ADDRESS = InetAddress.getByName("127.0.0.2");
            REMOTE_ADDRESS = InetAddress.getByName("127.0.0.3");
        }
        catch (UnknownHostException e)
        {
            throw new AssertionError(e);
        }
    }

    protected static ByteBuffer key(int k)
    {
        return ByteBufferUtil.bytes(k);
    }

    protected static Token token(int v)
    {
        return DatabaseDescriptor.getPartitioner().getToken(ByteBufferUtil.bytes(v));
    }

    protected static Range<Token> range(int left, int right)
    {
        return range(token(left), token(right));
    }

    protected static Range<Token> range(Token left, Token right)
    {
        return new Range<>(left, right);
    }

    protected static void printInstance(Instance instance)
    {
        printInstance(instance, null);
    }

    protected static void printInstance(Instance instance, String name)
    {
        name = name != null ? name + ": " : "";
        System.out.println(">>> " + name + instance.toString());

    }

    protected static final Token TOKEN0 = token(0);
    protected static final Token TOKEN100 = token(100);
    protected static final Range<Token> RANGE = new Range<>(TOKEN0, TOKEN100);
    protected static UUID CFID;

    static class DoNothing implements Runnable
    {
        public volatile int timesRun = 0;
        @Override
        public void run()
        {
            // not doing anything
            timesRun++;
        }
    }

    @BeforeClass
    public static void setUpClass() throws Exception
    {
        cfm = CFMetaData.compile("CREATE TABLE ks.tbl (k INT PRIMARY KEY, v INT);", "ks");
        thriftcf = CFMetaData.denseCFMetaData("ks", "thrifttbl", Int32Type.instance);
        Map<String, String> ksOpts = new HashMap<>();
        ksOpts.put("replication_factor", "1");
        ksm = KSMetaData.newKeyspace("ks", SimpleStrategy.class, ksOpts, true, Arrays.asList(cfm, thriftcf));
        Schema.instance.load(ksm);
        CFID = cfm.cfId;
    }

    private static void truncate(String table)
    {
        Keyspace.open(SystemKeyspace.NAME).getColumnFamilyStore(table).truncateBlocking();
    }

    protected static void clearAll()
    {
        truncate(SystemKeyspace.EPAXOS_INSTANCE);
        truncate(SystemKeyspace.EPAXOS_KEY_STATE);
        truncate(SystemKeyspace.EPAXOS_TOKEN_STATE);

        truncate(SystemKeyspace.PAXOS);
        truncate(SystemKeyspace.PAXOS_UPGRADE);
    }

    @Before
    public void cleanup()
    {
        clearAll();
    }

    protected MessageEnvelope<Instance> wrapInstance(Instance instance)
    {
        return wrapInstance(instance, 0);
    }

    protected MessageEnvelope<Instance> wrapInstance(Instance instance, long epoch)
    {
        return new MessageEnvelope<>(instance.getToken(), instance.getCfId(), epoch, instance.getScope(), instance);
    }

    protected static ThriftCASRequest getThriftCasRequest()
    {
        ColumnFamily expected = ArrayBackedSortedColumns.factory.create("ks", thriftcf.cfName);
        expected.addColumn(CellNames.simpleDense(ByteBufferUtil.bytes("v")), ByteBufferUtil.bytes(2), 3L);

        ColumnFamily updates = ArrayBackedSortedColumns.factory.create("ks", thriftcf.cfName);
        updates.addColumn(CellNames.simpleDense(ByteBufferUtil.bytes("v")), ByteBufferUtil.bytes(5), 6L);

        return new ThriftCASRequest(expected, updates);
    }

    protected static CQL3CasRequest getCqlCasRequest(String query, List<ByteBuffer> bindings, ConsistencyLevel consistencyLevel)
    {
        try
        {
            ModificationStatement.Parsed parsed = (ModificationStatement.Parsed) QueryProcessor.parseStatement(query);
            parsed.prepareKeyspace("ks");
            parsed.setQueryString(query);
            ParsedStatement.Prepared prepared = parsed.prepare();

            QueryOptions options = QueryOptions.create(consistencyLevel,
                                                       bindings,
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

    protected static CQL3CasRequest getCqlCasRequest(int k, int v, ConsistencyLevel consistencyLevel)
    {
        String query = "INSERT INTO ks.tbl (k, v) VALUES (?, ?) IF NOT EXISTS";
        List<ByteBuffer> bindings = Lists.newArrayList(ByteBufferUtil.bytes(k), ByteBufferUtil.bytes(v));
        return getCqlCasRequest(query, bindings, consistencyLevel);

    }

    protected static SerializedRequest newSerializedRequest(CASRequest request, ByteBuffer key)
    {
        return newSerializedRequest(request, key, ConsistencyLevel.SERIAL);
    }

    protected static SerializedRequest newSerializedRequest(CASRequest request, ByteBuffer key, ConsistencyLevel consistencyLevel)
    {
        SerializedRequest.Builder builder = SerializedRequest.builder();
        builder.casRequest(request);
        builder.cfName(cfm.cfName);
        builder.keyspaceName(cfm.ksName);
        builder.key(key);
        builder.consistencyLevel(consistencyLevel);
        return builder.build();
    }

    protected static SerializedRequest getSerializedThriftRequest()
    {
        ThriftCASRequest casRequest = getThriftCasRequest();
        return newSerializedRequest(casRequest, key(7));
    }

    protected static SerializedRequest getSerializedCQLRequest(int k, int v)
    {
        return getSerializedCQLRequest(k, v, ConsistencyLevel.SERIAL);
    }

    protected static SerializedRequest getSerializedCQLRequest(int k, int v, ConsistencyLevel cl)
    {
        CQL3CasRequest casRequest = getCqlCasRequest(k, v, cl);
        return newSerializedRequest(casRequest, casRequest.getKey(), cl);
    }

}
