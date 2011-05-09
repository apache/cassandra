package org.apache.cassandra.db.marshal;

import java.sql.Types;
import java.util.UUID;

public abstract class AbstractUUIDType extends AbstractType<UUID>
{
    public Class<UUID> getType()
    {
        return UUID.class;
    }

    public boolean isSigned()
    {
        return false;
    }

    public boolean isCaseSensitive()
    {
        return false;
    }

    public boolean isCurrency()
    {
        return false;
    }

    public int getPrecision(UUID obj)
    {
        return -1;
    }

    public int getScale(UUID obj)
    {
        return -1;
    }

    public int getJdbcType()
    {
        return Types.OTHER;
    }

    public boolean needsQuotes()
    {
        return false;
    }
}
