package org.apache.cassandra.service;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DefsTable;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.net.IAsyncResult;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.WrappedRunnable;

class MigrationTask extends WrappedRunnable
{
    private static final Logger logger = LoggerFactory.getLogger(MigrationTask.class);

    private static final MigrationTaskSerializer serializer = new MigrationTaskSerializer();

    private final InetAddress endpoint;

    MigrationTask(InetAddress endpoint)
    {
        this.endpoint = endpoint;
    }

    public void runMayThrow() throws Exception
    {
        MessageOut<MigrationTask> message = new MessageOut<MigrationTask>(MessagingService.Verb.MIGRATION_REQUEST,
                                                                          this,
                                                                          serializer);

        int retries = 0;
        while (retries < MigrationManager.MIGRATION_REQUEST_RETRIES)
        {
            if (!FailureDetector.instance.isAlive(endpoint))
            {
                logger.error("Can't send migration request: node {} is down.", endpoint);
                return;
            }

            IAsyncResult<Collection<RowMutation>> iar = MessagingService.instance().sendRR(message, endpoint);
            try
            {
                Collection<RowMutation> schema = iar.get(DatabaseDescriptor.getRpcTimeout(), TimeUnit.MILLISECONDS);
                DefsTable.mergeSchema(schema);
                return;
            }
            catch(TimeoutException e)
            {
                retries++;
            }
        }
    }

    private static class MigrationTaskSerializer implements IVersionedSerializer<MigrationTask>
    {
        public void serialize(MigrationTask task, DataOutput out, int version) throws IOException
        {
            // all recipient needs is our reply-to address, which it gets from the connection
        }

        public MigrationTask deserialize(DataInput in, int version) throws IOException
        {
            throw new UnsupportedOperationException();
        }

        public long serializedSize(MigrationTask migrationTask, int version)
        {
            return 0;
        }
    }
}
