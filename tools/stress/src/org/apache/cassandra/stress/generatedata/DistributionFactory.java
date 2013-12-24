package org.apache.cassandra.stress.generatedata;

import java.io.Serializable;

public interface DistributionFactory extends Serializable
{

    Distribution get();

}
