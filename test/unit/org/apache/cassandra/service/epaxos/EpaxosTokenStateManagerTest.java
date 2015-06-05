package org.apache.cassandra.service.epaxos;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.UUIDGen;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

public class EpaxosTokenStateManagerTest extends AbstractEpaxosTest
{

    private static final Token TOKEN100 = token(100);
    private static final Token TOKEN200 = token(200);
    private static final List<Token> TOKENS = ImmutableList.of(TOKEN100, TOKEN200);
    private static final Set<Range<Token>> TOKEN_SET;

    static
    {
        Set<Range<Token>> tokenSet = new HashSet<>();
        tokenSet.add(range(TOKEN0, TOKEN100));
        tokenSet.add(range(TOKEN100, TOKEN200));
        TOKEN_SET = ImmutableSet.copyOf(tokenSet);
    }

    private static final UUID CFID = UUIDGen.getTimeUUID();

    private static TokenStateManager getTokenStateManager()
    {
        return getTokenStateManager(Scope.GLOBAL);
    }

    private static TokenStateManager getTokenStateManager(Scope scope)
    {
        return new TokenStateManager(scope) {
            @Override
            protected Set<Range<Token>> getReplicatedRangesForCf(UUID cfId)
            {
                return TOKEN_SET;
            }
        };
    }

    @Test
    public void maybeInit()
    {
        TokenStateManager tsm = getTokenStateManager();

        Assert.assertFalse(tsm.managesCfId(CFID));

        TokenStateManager.ManagedCf cf = tsm.getOrInitManagedCf(CFID);
        List<Token> tokens = cf.allTokens();
        Assert.assertEquals(TOKENS, tokens);

        for (Token token: tokens)
        {
            TokenState ts = cf.get(token);
            Assert.assertEquals(token, ts.getToken());
            Assert.assertEquals(0, ts.getEpoch());
            Assert.assertEquals(0, ts.getExecutions());
        }
    }

    @Test
    public void addToken()
    {
        TokenStateManager tsm = getTokenStateManager();
        tsm.start();

        TokenState ts100 = tsm.get(TOKEN100, CFID);
        Assert.assertEquals(TOKEN100, ts100.getToken());
        ts100.setEpoch(5);

        TokenState ts200 = tsm.get(TOKEN200, CFID);
        Assert.assertEquals(TOKEN200, ts200.getToken());
        ts200.setEpoch(6);

        Token token150 = token(150);

        Assert.assertEquals(ts200, tsm.get(token150, CFID));

        TokenState ts150 = new TokenState(range(TOKEN100, token150), CFID, ts200.getEpoch(), 0);
        tsm.putState(ts150);

        Assert.assertEquals(token150, ts150.getToken());
        Assert.assertEquals(ts200.getEpoch(), ts150.getEpoch());
    }

    @Test
    public void unsavedExecutionThreshold()
    {
        final AtomicBoolean wasSaved = new AtomicBoolean(false);
        final int unsavedThreshold = 5;
        TokenStateManager tsm = new TokenStateManager(DEFAULT_SCOPE) {
            @Override
            protected Set<Range<Token>> getReplicatedRangesForCf(UUID cfId)
            {
                return TOKEN_SET;
            }

            @Override
            protected int getUnsavedExecutionThreshold(UUID cfId)
            {
                return unsavedThreshold;
            }

            @Override
            public void save(TokenState state)
            {
                wasSaved.set(true);
                super.save(state);
            }
        };
        tsm.setStarted();
        Token token = token(5);
        tsm.get(token, CFID);
        wasSaved.set(false);

        for (int i=0; i<unsavedThreshold+1; i++)
        {
            Assert.assertFalse(wasSaved.get());
            tsm.reportExecution(token, CFID);
        }
        Assert.assertTrue(wasSaved.get());
    }

    /**
     * When the token state manager starts up, if it encounters any token states
     * in a non-normal state, it means that C* was shut down while they were in
     * the process of recovering. In this case, the tsm should set their state to
     * recovery required.
     */
    @Test
    public void nonNormalStartupState()
    {
        // create token state and save it in with a non-normal state
        TokenStateManager tsm = getTokenStateManager();
        tsm.start();

        TokenState ts = tsm.get(TOKEN100, CFID);
        ts.setState(TokenState.State.PRE_RECOVERY);
        tsm.save(ts);

        // start another, and check that it changed the state on startup
        tsm = getTokenStateManager();
        tsm.start();

        ts = tsm.get(TOKEN100, CFID);
        Assert.assertEquals(TokenState.State.RECOVERY_REQUIRED, ts.getState());
    }

    @Test
    public void otherScopeIsntLoadedOnStart()
    {
        TokenStateManager global = new TokenStateManager(Scope.GLOBAL) {
            @Override
            protected Set<Range<Token>> getReplicatedRangesForCf(UUID cfId)
            {
                return Sets.newHashSet(new Range<Token>(token(0), token(100)));
            }
        };
        global.start();
        global.get(token(100), CFID);


        TokenStateManager local = new TokenStateManager(Scope.LOCAL) {
            @Override
            protected Set<Range<Token>> getReplicatedRangesForCf(UUID cfId)
            {
                return Sets.newHashSet(new Range<Token>(token(100), token(200)));
            }
        };
        local.start();
        local.get(token(200), CFID);

        Assert.assertEquals(1, global.numManagedTokensFor(CFID));
        Assert.assertEquals(1, local.numManagedTokensFor(CFID));

        String query = String.format("SELECT * FROM %s.%s", SystemKeyspace.NAME, SystemKeyspace.EPAXOS_TOKEN_STATE);
        Assert.assertEquals(2, QueryProcessor.executeInternal(query).size());

        TokenStateManager global2 = new TokenStateManager(Scope.GLOBAL);
        global2.start();

        TokenStateManager local2 = new TokenStateManager(Scope.LOCAL);
        local2.start();

        Assert.assertEquals(1, global2.numManagedTokensFor(CFID));
        Assert.assertEquals(1, local2.numManagedTokensFor(CFID));
    }

    @Test
    public void refreshInactive()
    {
        MockTokenStateManager tsm = new MockTokenStateManager(Scope.GLOBAL);
        tsm.setTokens(token(100), token(200), token(300), token(400));
        TokenStateManager.ManagedCf cf = tsm.getOrInitManagedCf(CFID, TokenState.State.INACTIVE);
        Assert.assertNull(cf.get(token(100)));
        Assert.assertNotNull(cf.get(token(200)));
        Assert.assertEquals(range(100, 200), cf.get(token(200)).getRange());
        Assert.assertNotNull(cf.get(token(300)));
        Assert.assertEquals(range(200, 300), cf.get(token(300)).getRange());
        Assert.assertNotNull(cf.get(token(400)));
        Assert.assertEquals(range(300, 400), cf.get(token(400)).getRange());

        cf.get(token(300)).setState(TokenState.State.NORMAL);
        cf.get(token(400)).setState(TokenState.State.NORMAL);

        tsm.setTokens(token(50), token(150), token(200), token(300), token(400));
        tsm.refreshInactive(CFID);

        Assert.assertNull(cf.get(token(50)));
        Assert.assertNull(cf.get(token(100)));
        Assert.assertNotNull(cf.get(token(150)));
        Assert.assertEquals(range(50, 150), cf.get(token(150)).getRange());
        Assert.assertNotNull(cf.get(token(200)));
        Assert.assertEquals(range(150, 200), cf.get(token(200)).getRange());
        Assert.assertNotNull(cf.get(token(300)));
        Assert.assertEquals(range(200, 300), cf.get(token(300)).getRange());
        Assert.assertNotNull(cf.get(token(400)));
        Assert.assertEquals(range(300, 400), cf.get(token(400)).getRange());

    }
}
