package org.apache.cassandra.db;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;

import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.marshal.AbstractCommutativeType;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

public class ReplicateOnWriteTask implements Runnable
{
    private RowMutation mutation;
    private static Logger logger = Logger.getLogger(ReplicateOnWriteTask.class);

    public ReplicateOnWriteTask(RowMutation mutation)
    {
        this.mutation = mutation;
    }

    public void run()
    {
        // construct SliceByNamesReadCommand for CFs to repair
        List<ReadCommand> readCommands = new LinkedList<ReadCommand>();
        for (ColumnFamily columnFamily : mutation.getColumnFamilies())
        {
            // filter out non-repair CFs
            if (!columnFamily.metadata().getReplicateOnWrite())
                continue;

            // CF type: regular
            if (!columnFamily.isSuper())
            {
                QueryPath queryPath = new QueryPath(columnFamily.metadata().cfName);
                ReadCommand readCommand = new SliceByNamesReadCommand(
                    mutation.getTable(),
                    mutation.key(),
                    queryPath,
                    columnFamily.getColumnNames()
                    );

                readCommands.add(readCommand);
                continue;
            }

            // CF type: super
            for (IColumn superColumn : columnFamily.getSortedColumns())
            {
                QueryPath queryPath = new QueryPath(columnFamily.metadata().cfName, superColumn.name());

                // construct set of sub-column names
                Collection<IColumn> subColumns = superColumn.getSubColumns();
                Collection<ByteBuffer> subColNames = new HashSet<ByteBuffer>(subColumns.size());
                for (IColumn subCol : subColumns)
                {
                    subColNames.add(subCol.name());
                }

                ReadCommand readCommand = new SliceByNamesReadCommand(
                    mutation.getTable(),
                    mutation.key(),
                    queryPath,
                    subColNames
                    );
                readCommands.add(readCommand);
            }
        }

        if (0 == readCommands.size())
            return;

        try
        {
            // send repair to non-local replicas
            List<InetAddress> foreignReplicas = StorageService.instance.getLiveNaturalEndpoints(
                mutation.getTable(),
                mutation.key()
                );
            foreignReplicas.remove(FBUtilities.getLocalAddress()); // remove local replica

            // create a repair RowMutation
            RowMutation repairRowMutation = new RowMutation(mutation.getTable(), mutation.key());
            for (ReadCommand readCommand : readCommands)
            {
                Table table = Table.open(readCommand.table);
                Row row = readCommand.getRow(table);
                AbstractType defaultValidator = row.cf.metadata().getDefaultValidator();
                if (defaultValidator.isCommutative())
                {
                    /**
                     * Clean out contexts for all nodes we're sending the repair to, otherwise,
                     * we could send a context which is local to one of the foreign replicas,
                     * which would then incorrectly add that to its own count, because
                     * local resolution aggregates.
                     */
                    // note: the following logic could be optimized
                    for (InetAddress foreignNode : foreignReplicas)
                    {
                        ((AbstractCommutativeType)defaultValidator).cleanContext(row.cf, foreignNode);
                    }
                }
                repairRowMutation.add(row.cf);
            }

            // send repair to non-local replicas
            for (InetAddress foreignReplica : foreignReplicas)
            {
                RowMutationMessage repairMessage = new RowMutationMessage(
                    repairRowMutation);
                Message message = repairMessage.makeRowMutationMessage(StorageService.Verb.REPLICATE_ON_WRITE);
                MessagingService.instance.sendOneWay(message, foreignReplica);
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
}
