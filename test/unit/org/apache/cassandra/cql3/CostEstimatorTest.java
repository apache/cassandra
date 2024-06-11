package org.apache.cassandra.optimizer;

import org.apache.cassandra.cql3.CQLStatement;
import static org.mockito.Mockito.*;
import static org.junit.Assert.*;
import org.junit.Test;

import java.util.List;

public class QueryPlannerTest {

    @Test
    public void testGeneratePlans() {
        QueryPlanner planner = new QueryPlanner();
        CQLStatement statement = mock(CQLStatement.class);
        Metadata metadata = new Metadata();
        metadata.setTableName("users");

        planner.addMetadata(statement, metadata);
        List<ExecutionPlan> plans = planner.getPlans();

        assertEquals(2, plans.size());
        assertEquals("Index Scan Plan", plans.get(0).getPlanName());
        assertEquals("Full Table Scan Plan", plans.get(1).getPlanName());
    }
}
