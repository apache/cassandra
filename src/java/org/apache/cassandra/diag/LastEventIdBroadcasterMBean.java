package org.apache.cassandra.diag;

import java.util.Map;

/**
 * Provides a list of event types and the corresponding highest event IDs. Consumers may these IDs to determine
 * if new data is available.
 *
 * <p>Example result</p>
 *
 * <table>
 *     <tr>
 *         <th>Event</th>
 *         <th>Last ID</th>
 *     </tr>
 *     <tr>
 *         <td>BootstrapEvent</td>
 *         <td>312</td>
 *     </tr>
 *     <tr>
 *         <td>CompactionEvent</td>
 *         <td>a53f9338-5f24-11e8-9c2d-fa7ae01bbebc</td>
 *     </tr>
 * </table>
 *
 * <p>Clients may either retrieve the current list of all events IDs, or make conditional requests for event IDs
 * based on the timestamp of the last update (much in the sense of e.g. HTTP's If-Modified-Since semantics).</p>
 */
public interface LastEventIdBroadcasterMBean
{
    /**
     * Retrieves a list of all event types and their highest IDs.
     */
    Map<String, Comparable> getLastEventIds();

    /**
     * Retrieves a list of all event types and their highest IDs, if updated since specified timestamp, or null.
     * @param lastUpdate timestamp to use to determine if IDs have been updated
     */
    Map<String, Comparable> getLastEventIdsIfModified(long lastUpdate);
}
