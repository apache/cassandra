/*
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
package org.apache.cassandra.db;

import java.security.MessageDigest;

import org.apache.cassandra.db.*;

/**
 * Groups the informations necessary to decide the liveness of a given piece of
 * column data.
 * <p>
 * In practice, a {@code LivenessInfo} groups 3 informations:
 *   1) the data timestamp. It is sometimes allowed for a given piece of data to have
 *      no timestamp (for {@link Row#partitionKeyLivenessInfo} more precisely), but if that
 *      is the case it means the data has no liveness info at all.
 *   2) the data ttl if relevant.
 *   2) the data local deletion time if relevant (that is, if either the data has a ttl or is deleted).
 */
public interface LivenessInfo extends Aliasable<LivenessInfo>
{
    public static final long NO_TIMESTAMP = Long.MIN_VALUE;
    public static final int NO_TTL = 0;
    public static final int NO_DELETION_TIME = Integer.MAX_VALUE;

    public static final LivenessInfo NONE = new SimpleLivenessInfo(NO_TIMESTAMP, NO_TTL, NO_DELETION_TIME);

    /**
     * The timestamp at which the data was inserted or {@link NO_TIMESTAMP}
     * if it has no timestamp (which may or may not be allowed).
     *
     * @return the liveness info timestamp.
     */
    public long timestamp();

    /**
     * Whether this liveness info has a timestamp or not.
     * <p>
     * Note that if this return {@code false}, then both {@link #hasTTL} and
     * {@link #hasLocalDeletionTime} must return {@code false} too.
     *
     * @return whether this liveness info has a timestamp or not.
     */
    public boolean hasTimestamp();

    /**
     * The ttl (if any) on the data or {@link NO_TTL} if the data is not
     * expiring.
     *
     * Please note that this value is the TTL that was set originally and is thus not
     * changing. If you want to figure out how much time the data has before it expires,
     * then you should use {@link #remainingTTL}.
     */
    public int ttl();

    /**
     * Whether this liveness info has a TTL or not.
     *
     * @return whether this liveness info has a TTL or not.
     */
    public boolean hasTTL();

    /**
     * The deletion time (in seconds) on the data if applicable ({@link NO_DELETION}
     * otherwise).
     *
     * There is 3 cases in practice:
     *   1) the data is neither deleted nor expiring: it then has neither {@code ttl()}
     *      nor {@code localDeletionTime()}.
     *   2) the data is expiring/expired: it then has both a {@code ttl()} and a
     *      {@code localDeletionTime()}. Whether it's still live or is expired depends
     *      on the {@code localDeletionTime()}.
     *   3) the data is deleted: it has no {@code ttl()} but has a
     *      {@code localDeletionTime()}.
     */
    public int localDeletionTime();

    /**
     * Whether this liveness info has a local deletion time or not.
     *
     * @return whether this liveness info has a local deletion time or not.
     */
    public boolean hasLocalDeletionTime();

    /**
     * The actual remaining time to live (in seconds) for the data this is
     * the liveness information of.
     *
     * {@code #ttl} returns the initial TTL sets on the piece of data while this
     * method computes how much time the data actually has to live given the
     * current time.
     *
     * @param nowInSec the current time in seconds.
     * @return the remaining time to live (in seconds) the data has, or
     * {@code -1} if the data is either expired or not expiring.
     */
    public int remainingTTL(int nowInSec);

    /**
     * Checks whether a given piece of data is live given the current time.
     *
     * @param nowInSec the current time in seconds.
     * @return whether the data having this liveness info is live or not.
     */
    public boolean isLive(int nowInSec);

    /**
     * Adds this liveness information to the provided digest.
     *
     * @param digest the digest to add this liveness information to.
     */
    public void digest(MessageDigest digest);

    /**
     * Validate the data contained by this liveness information.
     *
     * @throws MarshalException if some of the data is corrupted.
     */
    public void validate();

    /**
     * The size of the (useful) data this liveness information contains.
     *
     * @return the size of the data this liveness information contains.
     */
    public int dataSize();

    /**
     * Whether this liveness information supersedes another one (that is
     * whether is has a greater timestamp than the other or not).
     *
     * @param other the {@code LivenessInfo} to compare this info to.
     *
     * @return whether this {@code LivenessInfo} supersedes {@code other}.
     */
    public boolean supersedes(LivenessInfo other);

    /**
     * Returns the result of merging this info to another one (that is, it
     * return this info if it supersedes the other one, or the other one
     * otherwise).
     */
    public LivenessInfo mergeWith(LivenessInfo other);

    /**
     * Whether this liveness information can be purged.
     * <p>
     * A liveness info can be purged if it is not live and hasn't been so
     * for longer than gcGrace (or more precisely, it's local deletion time
     * is smaller than gcBefore, which is itself "now - gcGrace").
     *
     * @param maxPurgeableTimestamp the biggest timestamp that can be purged.
     * A liveness info will not be considered purgeable if its timestamp is
     * greater than this value, even if it mets the other criteria for purging.
     * @param gcBefore the local deletion time before which deleted/expired
     * liveness info can be purged.
     *
     * @return whether this liveness information can be purged.
     */
    public boolean isPurgeable(long maxPurgeableTimestamp, int gcBefore);

    /**
     * Returns a copy of this liveness info updated with the provided timestamp.
     *
     * @param newTimestamp the timestamp for the returned info.
     * @return if this liveness info has a timestamp, a copy of it with {@code newTimestamp}
     * as timestamp. If it has no timestamp however, this liveness info is returned
     * unchanged.
     */
    public LivenessInfo withUpdatedTimestamp(long newTimestamp);
}
