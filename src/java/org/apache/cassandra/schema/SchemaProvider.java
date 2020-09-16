package org.apache.cassandra.schema;

import javax.annotation.Nullable;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.UnknownTableException;
import org.apache.cassandra.io.sstable.Descriptor;

public interface SchemaProvider
{
    @Nullable
    Keyspace getKeyspaceInstance(String keyspaceName);

    void storeKeyspaceInstance(Keyspace keyspace);

    @Nullable
    KeyspaceMetadata getKeyspaceMetadata(String keyspaceName);

    @Nullable
    TableMetadata getTableMetadata(TableId id);

    @Nullable
    TableMetadata getTableMetadata(String keyspace, String table);

    default TableMetadata getExistingTableMetadata(TableId id) throws UnknownTableException
    {
        TableMetadata metadata = getTableMetadata(id);
        if (metadata != null)
            return metadata;

        String message =
            String.format("Couldn't find table with id %s. If a table was just created, this is likely due to the schema"
                          + "not being fully propagated.  Please wait for schema agreement on table creation.",
                          id);
        throw new UnknownTableException(message, id);
    }

    @Nullable
    TableMetadataRef getTableMetadataRef(String keyspace, String table);

    @Nullable
    TableMetadataRef getTableMetadataRef(TableId id);

    @Nullable
    default TableMetadataRef getTableMetadataRef(Descriptor descriptor)
    {
        return getTableMetadataRef(descriptor.ksname, descriptor.cfname);
    }
}
