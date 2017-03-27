package org.apache.cassandra.cql3;

import java.util.Locale;

/**
 * Created by coleman on 3/27/17.
 */
public class PolicyName extends KeyspaceElementName // TODO: ABAC check if this class works.
{
    private String name;

    public void setName(String name, boolean keepCase)
    {
        this.name = keepCase ? name : name.toLowerCase(Locale.US);
    }

    public boolean hasName()
    {
        return name != null;
    }

    public String getName()
    {
        return name;
    }

    public CFName getCFName()
    {
        CFName cfName = new CFName();
        if (hasKeyspace())
            cfName.setKeyspace(getKeyspace(), true);
        return cfName;
    }

    @Override
    public String toString()
    {
        return name;
    }
}
