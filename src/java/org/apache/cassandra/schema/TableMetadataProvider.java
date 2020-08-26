package org.apache.cassandra.schema;

import javax.annotation.Nullable;

import org.apache.cassandra.exceptions.UnknownTableException;

public interface TableMetadataProvider
{
    @Nullable
    TableMetadata getTableMetadata(TableId id);

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
}
