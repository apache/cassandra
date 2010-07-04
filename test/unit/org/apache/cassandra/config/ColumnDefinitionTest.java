package org.apache.cassandra.config;

import org.junit.Test;

public class ColumnDefinitionTest
{
    @Test
    public void testSerializeDeserialize() throws Exception
    {
        ColumnDefinition cd0 = new ColumnDefinition(
                "TestColumnDefinitionName0".getBytes("UTF8"),
                "org.apache.cassandra.config.RandomClass0",
                "random index name 0",
                "random index type 0");

        ColumnDefinition cd1 = new ColumnDefinition(
                "TestColumnDefinition1".getBytes("UTF8"),
                "org.apache.cassandra.config.RandomClass1",
                null,
                null);

        testSerializeDeserialize(cd0);
        testSerializeDeserialize(cd1);
    }

    protected void testSerializeDeserialize(ColumnDefinition cd) throws Exception
    {
        ColumnDefinition newCd = ColumnDefinition.deserialize(ColumnDefinition.serialize(cd));
        assert cd != newCd;
        assert cd.hashCode() == newCd.hashCode();
        assert cd.equals(newCd);
    }
}
