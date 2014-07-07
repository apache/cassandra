package org.apache.cassandra.stress.operations.userdefined;

import java.io.IOException;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.stress.generate.PartitionGenerator;
import org.apache.cassandra.stress.settings.OptionDistribution;
import org.apache.cassandra.stress.settings.StressSettings;
import org.apache.cassandra.stress.settings.ValidationType;
import org.apache.cassandra.stress.util.JavaDriverClient;
import org.apache.cassandra.stress.util.ThriftClient;
import org.apache.cassandra.stress.util.Timer;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.ThriftConversion;

public class SchemaQuery extends SchemaStatement
{

    public SchemaQuery(Timer timer, PartitionGenerator generator, StressSettings settings, Integer thriftId, PreparedStatement statement, ConsistencyLevel cl, ValidationType validationType)
    {
        super(timer, generator, settings, OptionDistribution.get("fixed(1)").get(), statement, thriftId, cl, validationType);
    }

    int execute(JavaDriverClient client) throws Exception
    {
        return client.getSession().execute(bindRandom(partitions.get(0))).all().size();
    }

    int execute(ThriftClient client) throws Exception
    {
        return client.execute_prepared_cql3_query(thriftId, partitions.get(0).getToken(), thriftRandomArgs(partitions.get(0)), ThriftConversion.toThrift(cl)).getRowsSize();
    }

    private class JavaDriverRun extends Runner
    {
        final JavaDriverClient client;

        private JavaDriverRun(JavaDriverClient client)
        {
            this.client = client;
        }

        public boolean run() throws Exception
        {
            ResultSet rs = client.getSession().execute(bindRandom(partitions.get(0)));
            validate(rs);
            rowCount = rs.all().size();
            partitionCount = Math.min(1, rowCount);
            return true;
        }
    }

    private class ThriftRun extends Runner
    {
        final ThriftClient client;

        private ThriftRun(ThriftClient client)
        {
            this.client = client;
        }

        public boolean run() throws Exception
        {
            CqlResult rs = client.execute_prepared_cql3_query(thriftId, partitions.get(0).getToken(), thriftRandomArgs(partitions.get(0)), ThriftConversion.toThrift(cl));
            validate(rs);
            rowCount = rs.getRowsSize();
            partitionCount = Math.min(1, rowCount);
            return true;
        }
    }

    @Override
    public void run(JavaDriverClient client) throws IOException
    {
        timeWithRetry(new JavaDriverRun(client));
    }

    @Override
    public void run(ThriftClient client) throws IOException
    {
        timeWithRetry(new ThriftRun(client));
    }

}
