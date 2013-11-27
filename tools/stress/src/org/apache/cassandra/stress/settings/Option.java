package org.apache.cassandra.stress.settings;

import java.util.List;

abstract class Option
{

    abstract boolean accept(String param);
    abstract boolean happy();
    abstract String shortDisplay();
    abstract String longDisplay();
    abstract List<String> multiLineDisplay();

    public int hashCode()
    {
        return getClass().hashCode();
    }

    public boolean equals(Object that)
    {
        return this.getClass() == that.getClass();
    }

}
