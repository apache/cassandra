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

package org.apache.cassandra.db.rows;

import java.util.Collection;
import java.util.Objects;
import java.util.function.Function;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.cassandra.cache.IMeasurableMemory;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.marshal.ByteArrayAccessor;
import org.apache.cassandra.db.tries.DeletionAwareTrie;
import org.apache.cassandra.db.tries.Direction;
import org.apache.cassandra.db.tries.RangeState;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

/// A tombstone marker used in trie-backed structures. Unlike [RangeTombstoneMarker], this does not include a position
/// in the clustering order, only deletion times.
/// There are two kinds of trie markers:
/// - covering markers, which have a deletion time applicable to some position that is not a boundary in the deletions
///   branch,
/// - boundary markers, which switch from one deletion time (represented as a covering marker) to a different one
///   (either of which may be null).
///
/// See [RangeState] for further explanation of the types of markers.
///
/// To be able to easily identify lower-level boundaries inside a deletion branch, we also have "level markers"
/// meta-markers. These can be added standalone or as part of a boundary. These level markers have no effect on the
/// applicable deletions, but are needed e.g. to be able to list deleted rows that have no live data.
///
/// In addition to these, we have a special form of boundary for point deletions, where the left and right side are the
/// same (often null), but there is a deletion time applicable to the exact point. These are used to mark deletions at
/// the lowest points of the represented data hierarchy where no further complexity can exist below the marked point
/// (e.g. deleted rows in a trie that does not extend to the cell level)
/// to improve efficiency compared to bracketing the point with boundaries on both sides.
///
/// To aid with some interpretation decisions, our covering markers also include a `deletionKind` which specifies the
/// type of deletion this range comes from (column, row, range or partition).
public interface TrieTombstoneMarker extends RangeState<TrieTombstoneMarker>, IMeasurableMemory
{
    /// The kind of covering deletion specifies the origin of the range that is in effect with a given deletion time.
    enum Kind
    {
        PARTITION,
        RANGE,
        ROW,
        COLUMN
        // we don't yet use cell-level tombstones
    }

    /// The deletion that is in effect to the left of the current position (i.e. preceding in the forward direction).
    /// For return path (ascent) positions this includes the current key itself and its children.
    Covering leftDeletion();
    /// The deletion that is in effect to the right of the current position (i.e. succeding in the forward direction).
    /// For normal (descent) positions this includes the position itself and its children.
    Covering rightDeletion();

    default boolean hasLevelMarker(LevelMarker level)
    {
        return false;
    }

    /// If this is a point deletion, the deletion that is in effect at the specific key (regardless if the current
    /// position is on the ascent or descent path). Null otherwise.
    default Covering pointDeletion()
    {
        return null;
    }

    /// The deletion that applies to the specific key on which we are positioned when going in the forward direction.
    /// Equivalent to `pointDeletion() != null ? pointDeletion() : rightDeletion()`
    default Covering applicableToPointForward()
    {
        return rightDeletion();
    }
    /// The deletion that applies to the specific key on which we are positioned when going in the reverse direction.
    /// Equivalent to `pointDeletion() != null ? pointDeletion() : leftDeletion()`
    default Covering applicableToPointReverse()
    {
        return leftDeletion();
    }

    @Override
    Covering precedingState(Direction direction);

    @Override
    default Covering succedingState(Direction direction)
    {
        return precedingState(direction.opposite());
    }

    /// Converts this marker to [RangeTombstoneMarker], assigning it a clustering position from its byte-comparable
    /// path in the trie. This method is only applicable to boundary markers and will use only sides of the marker that
    /// have the `RANGE` kind.
    RangeTombstoneMarker toRangeTombstoneMarker(ByteComparable clusteringPrefixAsByteComparable,
                                                ByteComparable.Version byteComparableVersion,
                                                ClusteringComparator comparator);


    /// Combine two markers and return the applicable combined state, obtained by getting the higher of the deletion
    /// times on both sides of the marker. For boundaries this may result in a covering state (when both sides become
    /// equal) which is not stored or reported.
    TrieTombstoneMarker mergeWith(TrieTombstoneMarker existing);

    /// Static version of [#mergeWith].
    static TrieTombstoneMarker mergeUpdate(TrieTombstoneMarker existing, @Nonnull TrieTombstoneMarker update)
    {
        return update.mergeWith(existing);
    }

    /// Merge multiple markers, yielding a combined state obtained by getting the highest of the deletion times on both
    /// sides.
    static TrieTombstoneMarker merge(Collection<TrieTombstoneMarker> markers)
    {
        TrieTombstoneMarker acc = null;
        for (TrieTombstoneMarker marker : markers)
        {
            if (acc == null)
                acc = marker;
            else
                acc = acc.mergeWith(marker);
        }
        return acc;
    }

    /// Apply an incoming marker and drop the parts of this marker that do not survive (i.e. supercede) the incoming
    /// deletion. The result may be null, this, or a partial version of this.
    @Nullable TrieTombstoneMarker dropShadowed(TrieTombstoneMarker deletion);

    /// Static version of [#dropShadowed].
    static @Nullable TrieTombstoneMarker dropShadowedUpdate(TrieTombstoneMarker existing, TrieTombstoneMarker deletion)
    {
        if (existing == null)
            return null;
        else
            return existing.dropShadowed(deletion);
    }

    /// Return a marker that has its timestamps adjusted to the given new value. If this is a boundary between two
    /// different deletions, this will convert it to a covering state which is not reported or stored.
    TrieTombstoneMarker withUpdatedTimestamp(long newTimestamp);

    /// Apply the given mapper to this marker. The mapper must convert equal deletion times to equal.
    @Nullable TrieTombstoneMarker map(Function<DeletionTime, DeletionTime> mapper);

    static Covering covering(DeletionTime deletionTime, Kind kind)
    {
        return new Covering(deletionTime, kind);
    }

    static Point point(DeletionTime deletionTime, Kind kind)
    {
        return new Point(covering(deletionTime, kind), null);
    }

    static Covering covering(long deletedAt, long localDeletionTime, Kind kind)
    {
        return new Covering(deletedAt, localDeletionTime, kind);
    }

    static Point point(long deletedAt, long localDeletionTime, Kind kind)
    {
        return new Point(covering(deletedAt, localDeletionTime, kind), null);
    }

    /// Returns `right` if `left` does not supersede it, `left` otherwise.
    static Covering combine(Covering left, Covering right)
    {
        if (left == null)
            return right;
        if (right == null)
            return left;
        if (left.supersedes(right))
            return left;
        else
            return right;
    }

    /// Returns `value` if it survives (i.e. supersedes) `deletion`.
    static Covering applyDeletion(Covering value, Covering deletion)
    {
        if (value == null)
            return null;
        if (deletion == null)
            return value;
        if (value.supersedes(deletion))
            return value;
        else
            return null;
    }

    /// Make a marker with a suitable type for the given parameters.
    static TrieTombstoneMarker make(Covering left, Covering right, LevelMarker levelMarkerIfPresent)
    {
        if (levelMarkerIfPresent == null)
        {
            if (left == right) // includes both being null
                return left;

            if (left != null && left.equals(right))
                return left;
        }
        else if (left == null && right == null)
            return levelMarkerIfPresent;

        return new Boundary(left, right, levelMarkerIfPresent);
    }

    private static RangeTombstoneMarker makeRangeTombstoneMarker(@Nullable Covering leftDeletion,
                                                                 @Nullable Covering rightDeletion,
                                                                 ByteComparable clusteringPrefixAsByteComparable,
                                                                 ByteComparable.Version byteComparableVersion,
                                                                 ClusteringComparator comparator)
    {
        assert byteComparableVersion == ByteComparable.Version.OSS50;
        if (leftDeletion == null || leftDeletion.deletionKind != Kind.RANGE)
        {
            if (rightDeletion == null || rightDeletion.deletionKind != Kind.RANGE)
                return null;
            else
                return new RangeTombstoneBoundMarker(comparator.boundFromByteComparable(ByteArrayAccessor.instance,
                                                                                        clusteringPrefixAsByteComparable,
                                                                                        false),
                                                     rightDeletion);
        }

        if (rightDeletion == null || rightDeletion.deletionKind != Kind.RANGE)
            return new RangeTombstoneBoundMarker(comparator.boundFromByteComparable(ByteArrayAccessor.instance,
                                                                                    clusteringPrefixAsByteComparable,
                                                                                    true),
                                                 leftDeletion);

        return new RangeTombstoneBoundaryMarker(comparator.boundaryFromByteComparable(ByteArrayAccessor.instance,
                                                                                      clusteringPrefixAsByteComparable),
                                                leftDeletion,
                                                rightDeletion);
    }

    /// Get the deletion in the given deletion-aware trie that applies to the given key.
    static Covering applicableDeletion(DeletionAwareTrie<Object, TrieTombstoneMarker> data, boolean deletionBranchMustBeAtRoot, ByteComparable key)
    {
        TrieTombstoneMarker marker = deletionMarker(data, deletionBranchMustBeAtRoot, key);
        return marker != null ? marker.applicableToPointForward() : null;
    }

    private static TrieTombstoneMarker deletionMarker(DeletionAwareTrie<Object, TrieTombstoneMarker> data, boolean deletionBranchMustBeAtRoot, ByteComparable key)
    {
        return deletionBranchMustBeAtRoot ? data.deletionBranchAtRoot().applicableRange(key) : data.applicableDeletion(key);
    }

    static Covering applicableDeletionAtRoot(DeletionAwareTrie<Object, TrieTombstoneMarker> data)
    {
        TrieTombstoneMarker marker = data.deletionAtRoot();
        return marker != null ? marker.applicableToPointForward() : null;
    }

    /// Get the deletion in the given deletion-aware trie that applies to the given key, returning DeletionTime.LIVE
    /// if none is found.
    static DeletionTime applicableDeletionOrLive(DeletionAwareTrie<Object, TrieTombstoneMarker> data, boolean deletionBranchMustBeAtRoot, ByteComparable key)
    {
        return applicableOrLive(deletionMarker(data, deletionBranchMustBeAtRoot, key));
    }

    /// Get the deletion in the given deletion-aware trie that applies to the root of the trie, returning DeletionTime.LIVE
    /// if none is found.
    static DeletionTime applicableDeletionOrLiveAtRoot(DeletionAwareTrie<Object, TrieTombstoneMarker> data)
    {
        return applicableOrLive(data.deletionAtRoot());
    }

    private static DeletionTime applicableOrLive(TrieTombstoneMarker marker)
    {
        if (marker != null)
        {
            Covering deletion = marker.applicableToPointForward();
            if (deletion != null)
                return deletion;
        }
        return DeletionTime.LIVE;
    }

    static class Covering extends DeletionTime implements TrieTombstoneMarker
    {
        static final long HEAP_SIZE = ObjectSizes.measure(new Covering(DeletionTime.LIVE, null));
        private final Kind deletionKind;

        private Covering(DeletionTime deletionTime, Kind kind)
        {
            super(deletionTime.markedForDeleteAt(), deletionTime.localDeletionTime());
            deletionKind = kind;
        }

        private Covering(long markedForDeleteAt, long localDeletionTime, Kind kind)
        {
            super(markedForDeleteAt, localDeletionTime);
            deletionKind = kind;
        }

        @Override
        public RangeTombstoneMarker toRangeTombstoneMarker(ByteComparable clusteringPrefixAsByteComparable,
                                                           ByteComparable.Version byteComparableVersion,
                                                           ClusteringComparator comparator)
        {
            throw new AssertionError("Covering trie tombstone cannot be converted to a RangeTombstoneMarker");
        }

        @Override
        public Covering leftDeletion()
        {
            return this;
        }

        @Override
        public Covering rightDeletion()
        {
            return this;
        }

        public Kind deletionKind()
        {
            return deletionKind;
        }

        @Override
        public TrieTombstoneMarker mergeWith(TrieTombstoneMarker other)
        {
            if (other instanceof Boundary)
                return other.mergeWith(this);
            if (other instanceof Point)
                return other.mergeWith(this);
            if (other instanceof LevelMarker)
                return new Boundary(this, this, (LevelMarker) other);

            return combine(this, (Covering) other);
        }

        @Override
        public TrieTombstoneMarker dropShadowed(TrieTombstoneMarker deletion)
        {
            if (deletion == null)
                return this;

            if (deletion instanceof Covering)
                return applyDeletion(this, (Covering) deletion);

            // We ignore point deletions and level marker in the incoming deletion as we don't have any to remove.
            TrieTombstoneMarker other = deletion;
            Covering newLeft = applyDeletion(this, other.leftDeletion());
            Covering newRight = applyDeletion(this, other.rightDeletion());
            return make(newLeft, newRight, null);
        }

        @Override
        public Covering withUpdatedTimestamp(long newTimestamp)
        {
            return new Covering(newTimestamp, localDeletionTime(), deletionKind);
        }

        @Override
        public @Nullable Covering map(Function<DeletionTime, DeletionTime> mapper)
        {
            DeletionTime mapped = mapper.apply(this);
            if (mapped == this)
                return this;
            if (mapped == null || mapped.isLive())
                return null;
            return new Covering(mapped, deletionKind);
        }

        @Override
        public boolean isBoundary()
        {
            return false;
        }

        @Override
        public Covering precedingState(Direction direction)
        {
            return this;
        }

        @Override
        public TrieTombstoneMarker restrict(boolean applicableBefore, boolean applicableAfter)
        {
            throw new AssertionError("Restrict is only applicable to boundary markers");
        }

        @Override
        public TrieTombstoneMarker asBoundary(Direction direction)
        {
            return direction.isForward() ? new Boundary(null, this, null) : new Boundary(this, null, null);
        }

        @Override
        public long unsharedHeapSize()
        {
            // Note: HEAP_SIZE is used directly by Point and Boundary. Make sure to apply any changes there too.
            return HEAP_SIZE;
        }

        @Override
        public String toString()
        {
            return super.toString() + '[' + deletionKind + ']';
        }

        // inherits equals and hashcode
    }

    static class Boundary implements TrieTombstoneMarker
    {
        // Every boundary contains one side of a deletion, and for simplicity we assume that any covering deletion we
        // interrupt is already accounted for by its end boundaries, so with every new Boundary we add this object's
        // size plus one half of a Covering.
        static final long UNSHARED_HEAP_SIZE =
            ObjectSizes.measure(new Boundary(new Covering(0, 0, null), null, null)) +
            Covering.HEAP_SIZE / 2;

        final @Nullable Covering leftDeletion;
        final @Nullable Covering rightDeletion;
        final @Nullable LevelMarker levelMarkerIfPresent;

        private Boundary(@Nullable Covering left, @Nullable Covering right, LevelMarker levelMarkerIfPresent)
        {
            assert left != null || right != null;
            assert left == null || !left.isLive();
            assert right == null || !right.isLive();
            this.leftDeletion = left;
            this.rightDeletion = right;
            this.levelMarkerIfPresent = levelMarkerIfPresent;
        }

        @Override
        public boolean hasLevelMarker(LevelMarker level)
        {
            return levelMarkerIfPresent == level;
        }

        @Override
        public RangeTombstoneMarker toRangeTombstoneMarker(ByteComparable clusteringPrefixAsByteComparable,
                                                           ByteComparable.Version byteComparableVersion,
                                                           ClusteringComparator comparator)
        {
            return makeRangeTombstoneMarker(leftDeletion,
                                            rightDeletion,
                                            clusteringPrefixAsByteComparable,
                                            byteComparableVersion,
                                            comparator);
        }

        @Override
        public TrieTombstoneMarker mergeWith(TrieTombstoneMarker existing)
        {
            if (existing == null)
                return this;

            if (existing instanceof Point)
                return existing.mergeWith(this);

            if (existing instanceof LevelMarker)
                return this.hasLevelMarker((LevelMarker) existing) ? this : new Boundary(leftDeletion, rightDeletion, (LevelMarker) existing);

            assert existing.pointDeletion() == null : "Unexpected point deletion in " + existing;
            Covering otherLeft = existing.leftDeletion();
            Covering newLeft = combine(leftDeletion, otherLeft);
            Covering otherRight = existing.rightDeletion();
            Covering newRight = combine(rightDeletion, otherRight);
            LevelMarker otherLevelMarker = (existing instanceof Boundary) ? ((Boundary) existing).levelMarkerIfPresent : null;
            LevelMarker newLevelMarker = levelMarkerIfPresent != null ? levelMarkerIfPresent : otherLevelMarker;

            if (leftDeletion == newLeft && rightDeletion == newRight && levelMarkerIfPresent == newLevelMarker)
                return this;
            if (otherLeft == newLeft && otherRight == newRight && newLevelMarker == otherLevelMarker)
                return existing;
            return make(newLeft, newRight, newLevelMarker);
        }

        @Override
        public TrieTombstoneMarker dropShadowed(TrieTombstoneMarker deletion)
        {
            if (deletion == null)
                return this;

            // We ignore point deletion in the incoming deletion as we don't have any to remove.
            Covering newLeft = applyDeletion(leftDeletion, deletion.leftDeletion());
            Covering newRight = applyDeletion(rightDeletion, deletion.rightDeletion());
            if (leftDeletion == newLeft && rightDeletion == newRight)
                return this;
            return make(newLeft, newRight, levelMarkerIfPresent);
        }

        @Override
        public TrieTombstoneMarker withUpdatedTimestamp(long newTimestamp)
        {
            Covering newLeft = leftDeletion != null ? leftDeletion.withUpdatedTimestamp(newTimestamp) : null;
            Covering newRight = rightDeletion != null ? rightDeletion.withUpdatedTimestamp(newTimestamp) : null;
            return make(newLeft, newRight, levelMarkerIfPresent);
        }

        @Override
        public @Nullable TrieTombstoneMarker map(Function<DeletionTime, DeletionTime> mapper)
        {
            Covering newLeft = leftDeletion != null ? leftDeletion.map(mapper) : null;
            Covering newRight = rightDeletion != null ? rightDeletion.map(mapper) : null;
            return make(newLeft, newRight, levelMarkerIfPresent);
        }

        @Override
        public boolean isBoundary()
        {
            return true;
        }

        @Override
        public Covering precedingState(Direction dir)
        {
            return dir.isForward() ? leftDeletion : rightDeletion;
        }

        @Override
        public TrieTombstoneMarker restrict(boolean applicableBefore, boolean applicableAfter)
        {
            if ((!applicableBefore || leftDeletion == null) && (!applicableAfter || rightDeletion == null))
                return levelMarkerIfPresent;
            if (applicableBefore && applicableAfter)
                return this;
            return new Boundary(applicableBefore ? leftDeletion : null,
                                applicableAfter ? rightDeletion : null,
                                levelMarkerIfPresent);
        }

        @Override
        public TrieTombstoneMarker asBoundary(Direction direction)
        {
            throw new AssertionError("Already a boundary");
        }

        @Override
        public Covering leftDeletion()
        {
            return leftDeletion;
        }

        @Override
        public Covering rightDeletion()
        {
            return rightDeletion;
        }

        @Override
        public String toString()
        {
            return (levelMarkerIfPresent != null ? levelMarkerIfPresent + " + " : "") +
                   (leftDeletion != null ? leftDeletion : "LIVE") +
                   " -> " +
                   (rightDeletion != null ? rightDeletion : "LIVE");
        }

        @Override
        public long unsharedHeapSize()
        {
            return UNSHARED_HEAP_SIZE;
        }

        @Override
        public boolean equals(Object o)
        {
            if (!(o instanceof Boundary)) return false;
            Boundary boundary = (Boundary) o;
            return Objects.equals(leftDeletion, boundary.leftDeletion) &&
                   Objects.equals(rightDeletion, boundary.rightDeletion) &&
                   levelMarkerIfPresent == boundary.levelMarkerIfPresent;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(leftDeletion, rightDeletion, levelMarkerIfPresent);
        }
    }

    /// Point deletion. Marks a deletion at the lowest points of the represented data hierarchy where no further
    /// complexity can exist below the marked point to improve efficiency compared to bracketing the point with
    /// boundaries on both sides.
    ///
    /// The point deletion applies only to the exact position of the marker (i.e. if there is substructure, this
    /// deletion will not be covering for the branch). `isBoundary` returns true even if the applicable covering
    /// deletion does not change, because the point must be reported as content.
    static class Point implements TrieTombstoneMarker
    {
        // Every point deletion introduces a new deletion time. If it interrupts an existing deletion, it will reuse
        // the Covering object provided by its end bounds. Thus, the unshared size is this object + the size of
        // one Covering.
        // If the point is also a boundary, we will add half a Covering size (see Boundary).
        static final long UNSHARED_HEAP_SIZE = ObjectSizes.measure(new Point(new Covering(0, 0, null),
                                                                             null)) +
                                               Covering.HEAP_SIZE;

        final @Nullable Covering leftDeletion;
        final @Nullable Covering rightDeletion;
        final Covering pointDeletion;

        public Point(Covering pointDeletion, @Nullable Covering coveringDeletion)
        {
            this(pointDeletion, coveringDeletion, coveringDeletion);
        }

        public Point(Covering pointDeletion, @Nullable Covering leftDeletion, @Nullable Covering rightDeletion)
        {
            assert pointDeletion != null;
            this.leftDeletion = leftDeletion;
            this.rightDeletion = rightDeletion;
            this.pointDeletion = pointDeletion;
        }

        @Override
        public Covering leftDeletion()
        {
            return leftDeletion;
        }

        @Override
        public Covering rightDeletion()
        {
            return rightDeletion;
        }


        @Override
        public Covering applicableToPointForward()
        {
            return pointDeletion;
        }

        @Override
        public Covering applicableToPointReverse()
        {
            return pointDeletion;
        }

        @Override
        public RangeTombstoneMarker toRangeTombstoneMarker(ByteComparable clusteringPrefixAsByteComparable,
                                                           ByteComparable.Version byteComparableVersion,
                                                           ClusteringComparator comparator)
        {
            if (leftDeletion == rightDeletion)
                return null;

            return TrieTombstoneMarker.makeRangeTombstoneMarker(leftDeletion,
                                                                    rightDeletion,
                                                                    clusteringPrefixAsByteComparable,
                                                                    byteComparableVersion,
                                                                    comparator);
        }

        @Override
        public TrieTombstoneMarker mergeWith(TrieTombstoneMarker existing)
        {
            if (existing == null)
                return this;

            if (existing.hasLevelMarker(LevelMarker.ROW))
                throw new AssertionError("Point deletion on a row marker is invalid");

            TrieTombstoneMarker existingMarker = existing;
            Covering point;
            Covering left = combine(leftDeletion, existingMarker.leftDeletion());
            Covering right = combine(rightDeletion, existingMarker.rightDeletion());

            if (existing instanceof Point)
            {
                Point existingPoint = (Point) existing;
                point = combine(pointDeletion, existingPoint.pointDeletion);
            }
            else if (existing instanceof Covering)
                point = applyDeletion(pointDeletion, (Covering) existingMarker);
            else
                point = dropIfCoveredByBoth(pointDeletion, existingMarker.leftDeletion(), existingMarker.rightDeletion());

            return updatedTo(point, left, right);
        }

        @Override
        public TrieTombstoneMarker dropShadowed(TrieTombstoneMarker deletion)
        {
            if (deletion == null)
                return this;

            TrieTombstoneMarker deletionMarker = deletion;
            Covering point;
            Covering left = applyDeletion(leftDeletion, deletionMarker.leftDeletion());
            Covering right = applyDeletion(rightDeletion, deletionMarker.rightDeletion());

            if (deletion instanceof Point)
            {
                Point deletionPoint = (Point) deletion;
                point = applyDeletion(pointDeletion, deletionPoint.pointDeletion);
            }
            else if (deletion instanceof Covering)
                point = applyDeletion(pointDeletion, (Covering) deletionMarker);
            else
                point = dropIfCoveredByBoth(pointDeletion, deletionMarker.leftDeletion(), deletionMarker.rightDeletion());

            return updatedTo(point, left, right);
        }

        @Override
        public Covering pointDeletion()
        {
            return pointDeletion;
        }

        @Override
        public TrieTombstoneMarker withUpdatedTimestamp(long newTimestamp)
        {
            if (leftDeletion != null && rightDeletion != null)
                return null; // point is subsumed by range deletion, and the boundary turns to covering which is not reported

            Covering left = leftDeletion != null ? new Covering(newTimestamp, leftDeletion.localDeletionTime(), leftDeletion.deletionKind) : null;
            Covering right = rightDeletion != null ? new Covering(newTimestamp, rightDeletion.localDeletionTime(), rightDeletion.deletionKind) : null;
            return new Point(new Covering(newTimestamp, pointDeletion.localDeletionTime(), pointDeletion.deletionKind), left, right);
        }

        @Override
        public @Nullable TrieTombstoneMarker map(Function<DeletionTime, DeletionTime> mapper)
        {
            Covering point = pointDeletion.map(mapper);
            Covering left = leftDeletion != null ? leftDeletion.map(mapper) : null;
            Covering right = rightDeletion != null ? rightDeletion.map(mapper) : null;
            point = dropIfCoveredByBoth(point, left, right);
            return updatedTo(point, left, right);
        }

        private Covering dropIfCoveredByBoth(Covering point, Covering left, Covering right)
        {
            return (left == null || right == null || point.supersedes(left) || point.supersedes(right))
                   ? point
                   : null;
        }

        private TrieTombstoneMarker updatedTo(Covering point, Covering left, Covering right)
        {
            if (point != null)
            {
                if (point == pointDeletion && left == leftDeletion && right == rightDeletion)
                    return this;
                else
                    return new Point(point, left, right);
            }
            else
                return make(left, right, null);
        }

        @Override
        public boolean isBoundary()
        {
            // Must be reported.
            return true;
        }

        @Override
        public Covering precedingState(Direction direction)
        {
            return direction.select(leftDeletion, rightDeletion);
        }

        @Override
        public TrieTombstoneMarker restrict(boolean applicableBefore, boolean applicableAfter)
        {
            Covering left = applicableBefore ? leftDeletion : null;
            Covering right = applicableAfter ? rightDeletion : null;
            if (left == leftDeletion && right == rightDeletion)
                return this;

            return new Point(pointDeletion, left, right);
        }

        @Override
        public TrieTombstoneMarker asBoundary(Direction direction)
        {
            throw new AssertionError("Cannot have a row clustering as slice bound.");
        }

        @Override
        public String toString()
        {
            if (leftDeletion == rightDeletion)
                return pointDeletion + (leftDeletion != null ? "(under " + leftDeletion + ")" : "");
            else
                return pointDeletion + " and "
                       + (leftDeletion != null ? leftDeletion : "LIVE") + " -> "
                       + (rightDeletion != null ? rightDeletion : "LIVE");

        }

        @Override
        public long unsharedHeapSize()
        {
            return UNSHARED_HEAP_SIZE + (leftDeletion != rightDeletion ? Covering.HEAP_SIZE / 2 : 0);
        }

        @Override
        public boolean equals(Object o)
        {
            if (!(o instanceof Point)) return false;
            Point point = (Point) o;
            return Objects.equals(leftDeletion, point.leftDeletion) &&
                   Objects.equals(rightDeletion, point.rightDeletion) &&
                   Objects.equals(pointDeletion, point.pointDeletion);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(leftDeletion, rightDeletion, pointDeletion);
        }
    }

    enum LevelMarker implements TrieTombstoneMarker
    {
        // We currently only need row level markers.
        ROW;

        @Override
        public boolean hasLevelMarker(LevelMarker level)
        {
            return this == level;
        }

        @Override
        public RangeTombstoneMarker toRangeTombstoneMarker(ByteComparable clusteringPrefixAsByteComparable,
                                                           ByteComparable.Version byteComparableVersion,
                                                           ClusteringComparator comparator)
        {
            return null;
        }

        @Override
        public Covering leftDeletion()
        {
            return null;
        }

        @Override
        public Covering rightDeletion()
        {
            return null;
        }

        @Override
        public TrieTombstoneMarker mergeWith(TrieTombstoneMarker existing)
        {
            if (existing == null || existing == this)
                return this;
            else if (existing instanceof LevelMarker)
                throw new AssertionError("Attempt to merge different level markers: " + this + " vs " + existing);
            else
                return existing.mergeWith(this);
        }

        @Nullable
        @Override
        public TrieTombstoneMarker dropShadowed(TrieTombstoneMarker deletion)
        {
            return this;
        }

        @Override
        public TrieTombstoneMarker withUpdatedTimestamp(long newTimestamp)
        {
            return this;
        }

        @Nullable
        @Override
        public TrieTombstoneMarker map(Function<DeletionTime, DeletionTime> mapper)
        {
            return this;
        }

        @Override
        public long unsharedHeapSize()
        {
            return 0;
        }

        @Override
        public boolean isBoundary()
        {
            return true; // to return it in toContent
        }

        @Override
        public Covering precedingState(Direction direction)
        {
            return null;
        }

        @Override
        public TrieTombstoneMarker restrict(boolean applicableBefore, boolean applicableAfter)
        {
            // Markers must be retained regardless of set coverage.
            return this;
        }

        @Override
        public TrieTombstoneMarker asBoundary(Direction direction)
        {
            throw new AssertionError("Already a boundary");
        }

        @Override
        public String toString()
        {
            return "Level " + super.toString();
        }
    }
}
