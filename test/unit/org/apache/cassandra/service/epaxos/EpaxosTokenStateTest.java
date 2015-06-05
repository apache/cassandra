package org.apache.cassandra.service.epaxos;

import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.utils.UUIDGen;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.UUID;

public class EpaxosTokenStateTest extends AbstractEpaxosTest
{
    private static final UUID CFID = UUIDGen.getTimeUUID();
    private static final Token TOKEN123 = token(123);

    private static Range<Token> rangeFor(Token right)
    {
        return range(TOKEN0, right);
    }

    @Test
    public void serialization() throws IOException
    {
        TokenState ts = new TokenState(range(TOKEN0, TOKEN123), CFID, 4, 6);
        DataOutputBuffer out = new DataOutputBuffer();
        TokenState.serializer.serialize(ts, out, 0);

        long expectedSize = out.getLength();
        Assert.assertEquals(expectedSize, TokenState.serializer.serializedSize(ts, 0));

        TokenState deserialized = TokenState.serializer.deserialize(ByteStreams.newDataInput(out.getData()), 0);
        Assert.assertEquals(ts.getToken(), deserialized.getToken());
        Assert.assertEquals(ts.getEpoch(), deserialized.getEpoch());
        Assert.assertEquals(ts.getExecutions(), deserialized.getExecutions());
    }

    /**
     * Test that getExecutions and getNumUnrecordedExecutions increment for each call to recordExecution
     */
    @Test
    public void recordExecutions()
    {
        TokenState ts = new TokenState(range(TOKEN0, TOKEN123), CFID, 0, 0);
        Assert.assertEquals(0, ts.getEpoch());
        Assert.assertEquals(0, ts.getExecutions());
        Assert.assertEquals(0, ts.getNumUnrecordedExecutions());

        ts.recordExecution();
        ts.recordExecution();
        ts.recordExecution();

        Assert.assertEquals(3, ts.getExecutions());
        Assert.assertEquals(3, ts.getNumUnrecordedExecutions());
    }

    @Test
    public void setEpochResetsExecutions()
    {
        TokenState ts = new TokenState(range(TOKEN0, TOKEN123), CFID, 4, 6);
        Assert.assertEquals(4, ts.getEpoch());
        Assert.assertEquals(6, ts.getExecutions());

        ts.setEpoch(5);
        Assert.assertEquals(0, ts.getExecutions());
        Assert.assertEquals(0, ts.getNumUnrecordedExecutions());
    }

    /**
     * Test getNumUnrecordedExecutions goes back to zero when saved
     */
    @Test
    public void onSave()
    {
        TokenState ts = new TokenState(range(TOKEN0, TOKEN123), CFID, 0, 0);
        Assert.assertEquals(0, ts.getEpoch());
        Assert.assertEquals(0, ts.getExecutions());
        Assert.assertEquals(0, ts.getNumUnrecordedExecutions());

        ts.recordExecution();
        ts.recordExecution();
        ts.recordExecution();

        Assert.assertEquals(3, ts.getNumUnrecordedExecutions());
        ts.onSave();
        Assert.assertEquals(0, ts.getNumUnrecordedExecutions());
    }

    /**
     * Check that correct token instance ids are returned, and removed
     * when epoch is changed
     */
    @Test
    public void epochInstances()
    {
        TokenState ts = new TokenState(range(TOKEN0, TOKEN123), CFID, 0, 0);
        UUID i0 = UUIDGen.getTimeUUID();
        UUID i1 = UUIDGen.getTimeUUID();
        UUID i2 = UUIDGen.getTimeUUID();

        ts.recordEpochInstance(0, i0);
        ts.recordEpochInstance(0, i1);
        ts.recordEpochInstance(1, i2);

        Assert.assertEquals(Sets.newHashSet(i0, i1, i2), ts.getCurrentEpochInstances());

        ts.setEpoch(1);

        Assert.assertEquals(Sets.newHashSet(i2), ts.getCurrentEpochInstances());
    }

    @Test
    public void tokenInstances()
    {
        Token t75 = token(75);
        Token t150 = token(150);
        Token t200 = token(200);
        TokenState ts = new TokenState(range(TOKEN0, t200), CFID, 0, 0);
        UUID tId0 = UUIDGen.getTimeUUID();
        UUID tId1 = UUIDGen.getTimeUUID();
        ts.recordTokenInstance(t75, tId0);
        ts.recordTokenInstance(t150, tId1);

        Assert.assertEquals(Sets.newHashSet(tId0, tId1), ts.getCurrentTokenInstances(rangeFor(token(151))));
        Assert.assertEquals(Sets.newHashSet(tId0, tId1), ts.getCurrentTokenInstances(rangeFor(token(150))));
        Assert.assertEquals(Sets.newHashSet(tId0), ts.getCurrentTokenInstances(rangeFor(token(149))));
        Assert.assertEquals(Sets.newHashSet(tId0), ts.getCurrentTokenInstances(rangeFor(token(75))));
        Assert.assertEquals(Sets.<UUID>newHashSet(), ts.getCurrentTokenInstances(rangeFor(token(50))));
    }
}
