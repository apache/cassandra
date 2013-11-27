package org.apache.cassandra.stress.generatedata;

import java.io.Serializable;

public interface DataGenFactory extends Serializable
{
    DataGen get();
}

