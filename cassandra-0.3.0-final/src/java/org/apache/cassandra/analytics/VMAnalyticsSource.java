/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.analytics;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.LogUtil;
import org.apache.log4j.Logger;


/**
 * This class sets up the analytics package to report metrics into
 * Ganglia for VM heap utilization.
 *
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com ) & Karthik Ranganathan ( kranganathan@facebook.com )
 */

public class VMAnalyticsSource implements IAnalyticsSource
{
	private static final String METRIC_MEMUSAGE = "VM Heap Utilization";
	private static final String RECORD_MEMUSAGE = "MemoryUsageRecord";
	private static final String TAG_MEMUSAGE = "MemoryUsageTag";
	private static final String TAG_MEMUSAGE_MEMUSED = "MemoryUsedTagValue";

	/**
	 * Setup the Ganglia record to display the VM heap utilization.
	 */
	public VMAnalyticsSource()
	{
		// set the units for the metric type
		AnalyticsContext.instance().setAttribute("units." + METRIC_MEMUSAGE, "MB");
		// create the record
        AnalyticsContext.instance().createRecord(RECORD_MEMUSAGE);
  	}

	/**
	 * Update the VM heap utilization record with the relevant data.
	 *
	 * @param context the reference to the context which has called this callback
	 */
	public void doUpdates(AnalyticsContext context)
	{
        // update the memory used record
		MetricsRecord memUsageRecord = context.getMetricsRecord(RECORD_MEMUSAGE);
		if(memUsageRecord != null)
		{
			updateUsedMemory(memUsageRecord);
		}
	}

	private void updateUsedMemory(MetricsRecord memUsageRecord)
	{
		memUsageRecord.setTag(TAG_MEMUSAGE, TAG_MEMUSAGE_MEMUSED);
		memUsageRecord.setMetric(METRIC_MEMUSAGE, getMemoryUsed());
		memUsageRecord.update();
	}

	private float getMemoryUsed()
	{
        MemoryMXBean memoryMxBean = ManagementFactory.getMemoryMXBean();
        MemoryUsage memUsage = memoryMxBean.getHeapMemoryUsage();
        return (float)memUsage.getUsed()/(1024 * 1024);
	}
}
