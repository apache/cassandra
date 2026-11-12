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

import java.util.Arrays;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;
import org.quicktheories.core.Gen;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.tries.Direction;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.integers;

/**
 * Unit tests for TrieTombstoneMarker and its implementations (Covering, Boundary, Point).
 */
public class TrieTombstoneMarkerTest
{
    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    // Test data generators
    private static final int MAX_TIMESTAMP = 1000000;
    private static final int MAX_LOCAL_DELETION_TIME = 100000;

    private Gen<DeletionTime> deletionTimeGen()
    {
        return integers().between(1, MAX_TIMESTAMP)
                        .zip(integers().between(1, MAX_LOCAL_DELETION_TIME),
                             (ts, ldt) -> DeletionTime.build(ts, ldt));
    }

    private Gen<DeletionTime> deletionTimeOrLiveGen()
    {
        return integers().between(0, MAX_TIMESTAMP)
                        .zip(integers().between(0, MAX_LOCAL_DELETION_TIME),
                             (ts, ldt) -> ts == 0 ? DeletionTime.LIVE : DeletionTime.build(ts, ldt));
    }

    // TODO: Test level markers

    // ========== Covering Marker Tests ==========

    @Test
    public void testCoveringCreation()
    {
        DeletionTime dt = DeletionTime.build(100, 50);
        TrieTombstoneMarker.Covering marker = TrieTombstoneMarker.covering(dt, TrieTombstoneMarker.Kind.RANGE);

        assertNotNull(marker);
        assertFalse(marker.isBoundary());
        assertEquals(marker, marker.precedingState(Direction.FORWARD));
        assertEquals(marker, marker.succedingState(Direction.FORWARD));
        assertEquals(TrieTombstoneMarker.Kind.RANGE, marker.deletionKind());
        assertEquals(dt, marker);
    }

    @Test
    public void testCoveringIsNotBoundary()
    {
        TrieTombstoneMarker covering = TrieTombstoneMarker.covering(DeletionTime.build(100, 50), TrieTombstoneMarker.Kind.RANGE);
        assertFalse("Covering marker should not be a boundary", covering.isBoundary());
    }

    @Test
    public void testCoveringPrecedingState()
    {
        TrieTombstoneMarker covering = TrieTombstoneMarker.covering(DeletionTime.build(100, 50), TrieTombstoneMarker.Kind.RANGE);
        
        // Covering markers return themselves as preceding state in both directions
        assertEquals(covering, covering.precedingState(Direction.FORWARD));
        assertEquals(covering, covering.precedingState(Direction.REVERSE));
        // Covering markers return themselves as succeding state in both directions
        assertEquals(covering, covering.succedingState(Direction.FORWARD));
        assertEquals(covering, covering.succedingState(Direction.REVERSE));
    }

    @Test
    public void testCoveringCannotConvertToRangeTombstoneMarker()
    {
        TrieTombstoneMarker covering = TrieTombstoneMarker.covering(DeletionTime.build(100, 50), TrieTombstoneMarker.Kind.RANGE);
        ClusteringComparator comparator = new ClusteringComparator(Int32Type.instance);
        
        try
        {
            covering.toRangeTombstoneMarker(ByteComparable.EMPTY,
                                           ByteComparable.Version.OSS50,
                                           comparator);
            fail("Covering marker should not be convertible to RangeTombstoneMarker");
        }
        catch (AssertionError e)
        {
            // Expected
        }
    }

    @Test
    public void testCoveringMergeWithCovering()
    {
        qt().forAll(deletionTimeGen(), deletionTimeGen())
            .checkAssert((dt1, dt2) -> {
                TrieTombstoneMarker m1 = TrieTombstoneMarker.covering(dt1, TrieTombstoneMarker.Kind.RANGE);
                TrieTombstoneMarker m2 = TrieTombstoneMarker.covering(dt2, TrieTombstoneMarker.Kind.RANGE);
                
                TrieTombstoneMarker merged = m1.mergeWith(m2);
                
                assertNotNull(merged);
                assertFalse(merged.isBoundary());
                
                // Should keep the higher deletion time
                DeletionTime expected = dt1.supersedes(dt2) ? dt1 : dt2;
                assertEquals(expected, merged);
            });
    }

    @Test
    public void testCoveringDropShadowed()
    {
        qt().forAll(deletionTimeGen(), deletionTimeGen())
            .checkAssert((dt1, dt2) -> {
                TrieTombstoneMarker marker = TrieTombstoneMarker.covering(dt1, TrieTombstoneMarker.Kind.RANGE);
                TrieTombstoneMarker deletion = TrieTombstoneMarker.covering(dt2, TrieTombstoneMarker.Kind.RANGE);
                
                TrieTombstoneMarker result = marker.dropShadowed(deletion);
                
                if (dt1.supersedes(dt2))
                {
                    // Marker survives if it supersedes the deletion
                    assertNotNull(result);
                    assertEquals(dt1, result);
                }
                else
                {
                    // Marker is dropped if deletion supersedes it
                    assertNull(result);
                }
            });
    }

    // ========== Boundary Marker Tests ==========

    @Test
    public void testBoundaryCreation()
    {
        DeletionTime left = DeletionTime.build(100, 50);
        DeletionTime right = DeletionTime.build(200, 60);
        
        TrieTombstoneMarker.Covering leftCov = TrieTombstoneMarker.covering(left, TrieTombstoneMarker.Kind.RANGE);
        TrieTombstoneMarker.Covering rightCov = TrieTombstoneMarker.covering(right, TrieTombstoneMarker.Kind.RANGE);
        
        TrieTombstoneMarker boundary = TrieTombstoneMarker.make(leftCov, rightCov, null);
        
        assertNotNull(boundary);
        assertTrue(boundary.isBoundary());
    }

    @Test
    public void testBoundaryWithEqualSidesBecomeCovering()
    {
        DeletionTime dt = DeletionTime.build(100, 50);
        TrieTombstoneMarker.Covering cov = TrieTombstoneMarker.covering(dt, TrieTombstoneMarker.Kind.RANGE);
        
        TrieTombstoneMarker result = TrieTombstoneMarker.make(cov, cov, null);
        
        assertNotNull(result);
        assertFalse("Equal sides should result in covering marker", result.isBoundary());
        assertEquals(dt, result);
    }

    @Test
    public void testBoundaryPrecedingState()
    {
        DeletionTime left = DeletionTime.build(100, 50);
        DeletionTime right = DeletionTime.build(200, 60);
        
        TrieTombstoneMarker.Covering leftCov = TrieTombstoneMarker.covering(left, TrieTombstoneMarker.Kind.RANGE);
        TrieTombstoneMarker.Covering rightCov = TrieTombstoneMarker.covering(right, TrieTombstoneMarker.Kind.RANGE);
        TrieTombstoneMarker boundary = TrieTombstoneMarker.make(leftCov, rightCov, null);
        
        assertEquals(leftCov, boundary.precedingState(Direction.FORWARD));
        assertEquals(rightCov, boundary.precedingState(Direction.REVERSE));

        assertEquals(rightCov, boundary.succedingState(Direction.FORWARD));
        assertEquals(leftCov, boundary.succedingState(Direction.REVERSE));
    }

    @Test
    public void testBoundaryMergeWithBoundary()
    {
        qt().forAll(deletionTimeOrLiveGen(), deletionTimeOrLiveGen(),
                   deletionTimeOrLiveGen(), deletionTimeOrLiveGen())
            .assuming((dt1, dt2, dt3, dt4) -> 
                !dt1.equals(dt2) && !dt3.equals(dt4)) // Ensure we have boundaries
            .checkAssert((dt1, dt2, dt3, dt4) -> {
                TrieTombstoneMarker.Covering left1 = dt1.isLive() ? null : TrieTombstoneMarker.covering(dt1, TrieTombstoneMarker.Kind.RANGE);
                TrieTombstoneMarker.Covering right1 = dt2.isLive() ? null : TrieTombstoneMarker.covering(dt2, TrieTombstoneMarker.Kind.RANGE);
                TrieTombstoneMarker.Covering left2 = dt3.isLive() ? null : TrieTombstoneMarker.covering(dt3, TrieTombstoneMarker.Kind.RANGE);
                TrieTombstoneMarker.Covering right2 = dt4.isLive() ? null : TrieTombstoneMarker.covering(dt4, TrieTombstoneMarker.Kind.RANGE);
                
                TrieTombstoneMarker b1 = TrieTombstoneMarker.make(left1, right1, null);
                TrieTombstoneMarker b2 = TrieTombstoneMarker.make(left2, right2, null);
                
                TrieTombstoneMarker merged = b1.mergeWith(b2);
                
                assertNotNull(merged);

                DeletionTime leftMax = merge(left1, left2);
                DeletionTime rightMax = merge(right1, right2);
                assertEquals(leftMax, merged.leftDeletion());
                assertEquals(rightMax, merged.rightDeletion());
            });
    }

    @Test
    public void testBoundaryRestrict()
    {
        DeletionTime left = DeletionTime.build(100, 50);
        DeletionTime right = DeletionTime.build(200, 60);
        
        TrieTombstoneMarker.Covering leftCov = TrieTombstoneMarker.covering(left, TrieTombstoneMarker.Kind.RANGE);
        TrieTombstoneMarker.Covering rightCov = TrieTombstoneMarker.covering(right, TrieTombstoneMarker.Kind.RANGE);
        TrieTombstoneMarker boundary = TrieTombstoneMarker.make(leftCov, rightCov, null);
        
        // Restrict to before only
        TrieTombstoneMarker beforeOnly = boundary.restrict(true, false);
        assertNotNull(beforeOnly);
        assertTrue(beforeOnly.isBoundary());
        assertEquals(leftCov, beforeOnly.leftDeletion());
        assertEquals(null, beforeOnly.rightDeletion());

        // Restrict to after only
        TrieTombstoneMarker afterOnly = boundary.restrict(false, true);
        assertNotNull(afterOnly);
        assertTrue(afterOnly.isBoundary());
        assertEquals(null, afterOnly.leftDeletion());
        assertEquals(rightCov, afterOnly.rightDeletion());

        // Restrict to both (should return same)
        TrieTombstoneMarker both = boundary.restrict(true, true);
        assertSame(boundary, both);
        
        // Restrict to neither (should return null)
        TrieTombstoneMarker neither = boundary.restrict(false, false);
        assertNull(neither);
    }

    // ========== Point Marker Tests ==========

    @Test
    public void testPointCreation()
    {
        DeletionTime pointDt = DeletionTime.build(150, 55);
        TrieTombstoneMarker point = TrieTombstoneMarker.point(pointDt, TrieTombstoneMarker.Kind.ROW);
        
        assertNotNull(point);
        assertTrue(point.isBoundary());
        assertEquals(TrieTombstoneMarker.Kind.ROW, point.applicableToPointForward().deletionKind());
        assertEquals(pointDt, point.applicableToPointForward());
    }

    @Test
    public void testPointWithCoveringDeletion()
    {
        DeletionTime pointDt = DeletionTime.build(150, 55);
        DeletionTime coveringDt = DeletionTime.build(100, 50);
        
        TrieTombstoneMarker.Covering pointCov = TrieTombstoneMarker.covering(pointDt, TrieTombstoneMarker.Kind.ROW);
        TrieTombstoneMarker.Covering coveringCov = TrieTombstoneMarker.covering(coveringDt, TrieTombstoneMarker.Kind.RANGE);
        
        TrieTombstoneMarker point = new TrieTombstoneMarker.Point(pointCov, coveringCov);
        
        assertNotNull(point);
        assertEquals(TrieTombstoneMarker.Kind.ROW, point.applicableToPointForward().deletionKind());
        assertEquals(pointDt, point.applicableToPointForward());
    }

    @Test
    public void testPointMergeWithCovering()
    {
        DeletionTime pointDt = DeletionTime.build(150, 55);
        DeletionTime coveringDt = DeletionTime.build(100, 50);
        
        TrieTombstoneMarker point = TrieTombstoneMarker.point(pointDt, TrieTombstoneMarker.Kind.ROW);
        TrieTombstoneMarker covering = TrieTombstoneMarker.covering(coveringDt, TrieTombstoneMarker.Kind.RANGE);
        
        TrieTombstoneMarker merged = point.mergeWith(covering);
        
        assertNotNull(merged);
        assertEquals(TrieTombstoneMarker.Kind.ROW, merged.applicableToPointForward().deletionKind());
        assertEquals(pointDt, merged.applicableToPointForward());
        
        // Point should survive if it supersedes covering
        if (pointDt.supersedes(coveringDt))
        {
            assertEquals(pointDt, merged.pointDeletion());
        }
    }

    @Test
    public void testPointMergeWithPoint()
    {
        qt().forAll(deletionTimeGen(), deletionTimeGen())
            .checkAssert((dt1, dt2) -> {
                TrieTombstoneMarker p1 = TrieTombstoneMarker.point(dt1, TrieTombstoneMarker.Kind.ROW);
                TrieTombstoneMarker p2 = TrieTombstoneMarker.point(dt2, TrieTombstoneMarker.Kind.ROW);
                
                TrieTombstoneMarker merged = p1.mergeWith(p2);
                
                assertNotNull(merged);
                assertEquals(TrieTombstoneMarker.Kind.ROW, merged.applicableToPointForward().deletionKind());

                // Should keep the higher deletion time
                DeletionTime expected = dt1.supersedes(dt2) ? dt1 : dt2;
                assertEquals(expected, merged.applicableToPointForward());
            });
    }

    @Test
    public void testPointDropShadowed()
    {
        qt().forAll(deletionTimeGen(), deletionTimeGen())
            .checkAssert((pointDt, deletionDt) -> {
                TrieTombstoneMarker point = TrieTombstoneMarker.point(pointDt, TrieTombstoneMarker.Kind.ROW);
                TrieTombstoneMarker deletion = TrieTombstoneMarker.covering(deletionDt, TrieTombstoneMarker.Kind.RANGE);
                
                TrieTombstoneMarker result = point.dropShadowed(deletion);
                
                if (pointDt.supersedes(deletionDt))
                {
                    // Point survives if it supersedes the deletion
                    assertNotNull(result);
                    assertEquals(TrieTombstoneMarker.Kind.ROW, result.applicableToPointForward().deletionKind());
                    assertEquals(pointDt, result.applicableToPointForward());
                }
                else
                {
                    // Point is dropped if deletion supersedes it
                    assertNull(result);
                }
            });
    }

    // ========== Collection Merge Tests ==========

    @Test
    public void testMergeCollection()
    {
        DeletionTime dt1 = DeletionTime.build(100, 50);
        DeletionTime dt2 = DeletionTime.build(200, 60);
        DeletionTime dt3 = DeletionTime.build(150, 55);
        
        List<TrieTombstoneMarker> markers = Arrays.asList(
            TrieTombstoneMarker.covering(dt1, TrieTombstoneMarker.Kind.RANGE),
            TrieTombstoneMarker.covering(dt2, TrieTombstoneMarker.Kind.RANGE),
            TrieTombstoneMarker.covering(dt3, TrieTombstoneMarker.Kind.RANGE)
        );
        
        TrieTombstoneMarker merged = TrieTombstoneMarker.merge(markers);
        
        assertNotNull(merged);
        // Should have the highest deletion time
        assertEquals(dt2, merged);
    }

    @Test
    public void testMergeEmptyCollection()
    {
        TrieTombstoneMarker merged = TrieTombstoneMarker.merge(Arrays.asList());
        assertNull(merged);
    }

    // ========== Timestamp Update Tests ==========

    @Test
    public void testCoveringWithUpdatedTimestamp()
    {
        DeletionTime original = DeletionTime.build(100, 50);
        TrieTombstoneMarker marker = TrieTombstoneMarker.covering(original, TrieTombstoneMarker.Kind.RANGE);
        
        long newTimestamp = 200;
        TrieTombstoneMarker updated = marker.withUpdatedTimestamp(newTimestamp);
        
        assertNotNull(updated);
        assertEquals(newTimestamp, updated.applicableToPointForward().markedForDeleteAt());
        assertEquals(original.localDeletionTime(), updated.applicableToPointForward().localDeletionTime());
    }

    @Test
    public void testPointWithUpdatedTimestamp()
    {
        DeletionTime pointDt = DeletionTime.build(150, 55);
        TrieTombstoneMarker point = TrieTombstoneMarker.point(pointDt, TrieTombstoneMarker.Kind.ROW);
        
        long newTimestamp = 250;
        TrieTombstoneMarker updated = point.withUpdatedTimestamp(newTimestamp);
        
        if (updated != null)
        {
            assertEquals(newTimestamp, updated.pointDeletion().markedForDeleteAt());
            assertEquals(pointDt.localDeletionTime(), updated.pointDeletion().localDeletionTime());
        }
    }

    // ========== Map Function Tests ==========

    @Test
    public void testCoveringMap()
    {
        DeletionTime original = DeletionTime.build(100, 50);
        TrieTombstoneMarker marker = TrieTombstoneMarker.covering(original, TrieTombstoneMarker.Kind.RANGE);
        
        // Map to a higher timestamp
        TrieTombstoneMarker mapped = marker.map(dt -> DeletionTime.build(dt.markedForDeleteAt() + 100, dt.localDeletionTime()));
        
        assertNotNull(mapped);
        assertEquals(200, mapped.applicableToPointForward().markedForDeleteAt());
    }

    @Test
    public void testCoveringMapToLive()
    {
        DeletionTime original = DeletionTime.build(100, 50);
        TrieTombstoneMarker marker = TrieTombstoneMarker.covering(original, TrieTombstoneMarker.Kind.RANGE);
        
        // Map to LIVE
        TrieTombstoneMarker mapped = marker.map(dt -> DeletionTime.LIVE);
        
        assertNull("Mapping to LIVE should return null", mapped);
    }

    @Test
    public void testPointMap()
    {
        DeletionTime pointDt = DeletionTime.build(150, 55);
        TrieTombstoneMarker point = TrieTombstoneMarker.point(pointDt, TrieTombstoneMarker.Kind.ROW);
        
        // Map to a higher timestamp
        TrieTombstoneMarker mapped = point.map(dt -> DeletionTime.build(dt.markedForDeleteAt() + 100, dt.localDeletionTime()));
        
        if (mapped != null)
        {
            assertNotNull(mapped.pointDeletion());
            assertEquals(250, mapped.pointDeletion().markedForDeleteAt());
        }
    }

    // ========== Memory Size Tests ==========

    @Test
    public void testCoveringMemorySize()
    {
        TrieTombstoneMarker covering = TrieTombstoneMarker.covering(DeletionTime.build(100, 50), TrieTombstoneMarker.Kind.RANGE);
        long size = covering.unsharedHeapSize();
        
        assertTrue("Covering marker should have positive heap size", size > 0);
    }

    @Test
    public void testBoundaryMemorySize()
    {
        DeletionTime left = DeletionTime.build(100, 50);
        DeletionTime right = DeletionTime.build(200, 60);
        
        TrieTombstoneMarker.Covering leftCov = TrieTombstoneMarker.covering(left, TrieTombstoneMarker.Kind.RANGE);
        TrieTombstoneMarker.Covering rightCov = TrieTombstoneMarker.covering(right, TrieTombstoneMarker.Kind.RANGE);
        TrieTombstoneMarker boundary = TrieTombstoneMarker.make(leftCov, rightCov, null);
        
        long size = boundary.unsharedHeapSize();
        
        assertTrue("Boundary marker should have positive heap size", size > 0);
        assertTrue("Boundary should be larger than covering", size >
                                                              TrieTombstoneMarker.covering(left, TrieTombstoneMarker.Kind.RANGE).unsharedHeapSize());
    }

    @Test
    public void testPointMemorySize()
    {
        TrieTombstoneMarker point = TrieTombstoneMarker.point(DeletionTime.build(150, 55), TrieTombstoneMarker.Kind.ROW);
        long size = point.unsharedHeapSize();
        
        assertTrue("Point marker should have positive heap size", size > 0);
    }

    // ========== Property-Based Tests ==========

    @Test
    public void testMergeIsCommutative()
    {
        qt().forAll(deletionTimeGen(), deletionTimeGen())
            .checkAssert((dt1, dt2) -> {
                TrieTombstoneMarker m1 = TrieTombstoneMarker.covering(dt1, TrieTombstoneMarker.Kind.RANGE);
                TrieTombstoneMarker m2 = TrieTombstoneMarker.covering(dt2, TrieTombstoneMarker.Kind.RANGE);
                
                TrieTombstoneMarker merged1 = m1.mergeWith(m2);
                TrieTombstoneMarker merged2 = m2.mergeWith(m1);
                
                assertEquals("Merge should be commutative", 
                           merged1, merged2);
            });
    }

    @Test
    public void testMergeIsAssociative()
    {
        qt().forAll(deletionTimeGen(), deletionTimeGen(), deletionTimeGen())
            .checkAssert((dt1, dt2, dt3) -> {
                TrieTombstoneMarker m1 = TrieTombstoneMarker.covering(dt1, TrieTombstoneMarker.Kind.RANGE);
                TrieTombstoneMarker m2 = TrieTombstoneMarker.covering(dt2, TrieTombstoneMarker.Kind.RANGE);
                TrieTombstoneMarker m3 = TrieTombstoneMarker.covering(dt3, TrieTombstoneMarker.Kind.RANGE);
                
                TrieTombstoneMarker merged1 = m1.mergeWith(m2).mergeWith(m3);
                TrieTombstoneMarker merged2 = m1.mergeWith(m2.mergeWith(m3));
                
                assertEquals("Merge should be associative", 
                           merged1, merged2);
            });
    }

    @Test
    public void testDropShadowedIsIdempotent()
    {
        qt().forAll(deletionTimeGen(), deletionTimeGen())
            .checkAssert((markerDt, deletionDt) -> {
                TrieTombstoneMarker marker = TrieTombstoneMarker.covering(markerDt, TrieTombstoneMarker.Kind.RANGE);
                TrieTombstoneMarker deletion = TrieTombstoneMarker.covering(deletionDt, TrieTombstoneMarker.Kind.RANGE);
                
                TrieTombstoneMarker dropped1 = marker.dropShadowed(deletion);
                TrieTombstoneMarker dropped2 = dropped1 != null ? dropped1.dropShadowed(deletion) : null;
                
                assertEquals("dropShadowed should be idempotent", dropped1, dropped2);
            });
    }

    private static DeletionTime merge(DeletionTime d1, DeletionTime d2)
    {
        return d2.supersedes(d1) ? d2 : d1;
    }
}
