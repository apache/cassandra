package org.apache.cassandra.stress.settings;

public enum CqlVersion
{

    NOCQL(null),
    CQL2("2.0.0"),
    CQL3("3.0.0");

    public final String connectVersion;

    private CqlVersion(String connectVersion)
    {
        this.connectVersion = connectVersion;
    }

    static CqlVersion get(String version)
    {
        if (version == null)
            return NOCQL;
        switch(version.charAt(0))
        {
            case '2':
                return CQL2;
            case '3':
                return CQL3;
            default:
                throw new IllegalStateException();
        }
    }

    public boolean isCql()
    {
        return this != NOCQL;
    }

    public boolean isCql2()
    {
        return this == CQL2;
    }

    public boolean isCql3()
    {
        return this == CQL3;
    }

}

