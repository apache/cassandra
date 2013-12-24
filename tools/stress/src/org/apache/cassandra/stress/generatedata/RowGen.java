package org.apache.cassandra.stress.generatedata;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Generates a row of data, by constructing one byte buffers per column according to some algorithm
 * and delegating the work of populating the values of those byte buffers to the provided data generator
 */
public abstract class RowGen
{

    final DataGen dataGen;
    protected RowGen(DataGen dataGenerator)
    {
        this.dataGen = dataGenerator;
    }

    public List<ByteBuffer> generate(long operationIndex)
    {
        List<ByteBuffer> fill = getColumns(operationIndex);
        dataGen.generate(fill, operationIndex);
        return fill;
    }

    // these byte[] may be re-used
    abstract List<ByteBuffer> getColumns(long operationIndex);

    abstract public boolean isDeterministic();

}
