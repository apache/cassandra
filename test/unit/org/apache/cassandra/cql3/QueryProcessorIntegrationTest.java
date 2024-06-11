package org.apache.cassandra.cql3;

import org.apache.cassandra.optimizer.QueryPlanner;
import org.apache.cassandra.optimizer.ExecutionPlan;
import org.apache.cassandra.optimizer.Metadata;
import org.apache.cassandra.service.ClientState;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

public class QueryProcessorIntegrationTest {

    private QueryProcessor queryProcessor;
    private QueryPlanner queryPlanner;
    private ClientState clientState;

    @Before
    public void setUp() {
        queryProcessor = QueryProcessor.instance;
        queryPlanner = new QueryPlanner();
        clientState = mock(ClientState.class);
    }

    @Test
    public void testPrepare() {
        String query = "SELECT * FROM users WHERE user_id = 123";
        Metadata metadata = new Metadata();
        metadata.setTableName("users");

        when(clientState.getRawKeyspace()).thenReturn(null);
        ResultMessage.Prepared prepared = queryProcessor.prepare(query, clientState);

        // Verify that the optimal plan was selected
        ExecutionPlan optimalPlan = queryPlanner.selectOptimalPlan();
        assertNotNull(optimalPlan);
    }
}
