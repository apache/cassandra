package org.apache.cassandra.optimizer;

import static org.junit.Assert.*;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class PlanSelectorTest {

    @Test
    public void testSelectBestPlan() {
        PlanSelector selector = new PlanSelector();
        ExecutionPlan plan1 = new ExecutionPlan("Plan 1", new String[]{"index scan"});
        ExecutionPlan plan2 = new ExecutionPlan("Plan 2", new String[]{"full table scan"});

        Map<ExecutionPlan, Integer> planCosts = new HashMap<>();
        planCosts.put(plan1, 1);
        planCosts.put(plan2, 10);

        ExecutionPlan bestPlan = selector.selectBestPlan(planCosts);

        assertEquals("Plan 1", bestPlan.getPlanName());
    }
}
