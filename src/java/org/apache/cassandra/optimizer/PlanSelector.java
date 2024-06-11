package org.apache.cassandra.optimizer;

import java.util.Map;

import org.apache.cassandra.cql3.QueryProcessor.ExecutionPlan;

public class PlanSelector {

    public static ExecutionPlan selectBestPlan(Map<ExecutionPlan, Integer> planCosts) {
        ExecutionPlan bestPlan = null;
        int lowestCost = Integer.MAX_VALUE;

        for (Map.Entry<ExecutionPlan, Integer> entry : planCosts.entrySet()) {
            if (entry.getValue() < lowestCost) {
                lowestCost = entry.getValue();
                bestPlan = entry.getKey();
            }
        }

        return bestPlan;
    }
}
