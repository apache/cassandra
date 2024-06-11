package org.apache.cassandra.optimizer;

import static org.junit.Assert.*;
import org.junit.Test;

public class ExecutionPlanTest {

    @Test
    public void testExecutionPlan() {
        String[] steps = {"Step 1: Use index scan", "Step 2: Filter results"};
        ExecutionPlan plan = new ExecutionPlan("Index Scan Plan", steps);

        assertEquals("Index Scan Plan", plan.getPlanName());
        assertArrayEquals(steps, plan.getSteps());
    }
}
