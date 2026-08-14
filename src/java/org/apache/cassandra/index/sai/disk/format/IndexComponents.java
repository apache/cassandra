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

package org.apache.cassandra.index.sai.disk.format;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.lifecycle.Tracker;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

/**
 * Represents a related group of concrete SAI components files which are either all the per-sstable components of a
 * given sstable, or all the components of a particular column index (for a given sstable).
 * <p>
 * The members of a component group correspond to actual SAI component files on disk, and are components that are
 * written together. A group is identified by:
 * <ul>
 *     <li>the sstable it is the components of (identified by the sstable {@link #descriptor()})</li>
 *     <li>the index ({@link #context()}) for the components, which is {@code null} for the per-sstable components</li>
 *     <li>the version the components are using. All the components of a group are on the same version</li>
 *     <li>the generation, within the version, of the group/components. Generations are used when
 *     {@link CassandraRelevantProperties#IMMUTABLE_SAI_COMPONENTS} is used, to avoid new builds to override (and thus
 *     mutate) older builds. When the option of immutable components is not used, then the generation is fixed to 0.
 *     See below for more details.</li>
 * </ul>
 * <p>
 *
 * <h2>Immutable components</h2>
 *
 * As mentioned above, when {@link CassandraRelevantProperties#IMMUTABLE_SAI_COMPONENTS} is enabled, existing components
 * are not overwritten by new builds (the underlying intent is to allow rebuilding without stopping reads (to the
 * rebuilt indexes). But unless a rebuild uses a different version than the existing components, the new components
 * need "something" to distinguish them from the old ones, and it is what the generation provides.
 * <p>
 * The generation is specific to a component group, meaning that all the components of a group share the same
 * generation (or to put it another way, if 2 components only differ by their generation, then they belong to different
 * build and thus different groups), but that different groups can have different generations. For instance, if a
 * specific index is rebuilt but without rebuilding the per-sstable components of each sstable, then after that rebuild
 * the per-sstable groups will have generation 0, but the index groups will have generation 1.
 * <p>
 * When a sstable is "loaded", the "active" set of components to use is based on finding, for each kind of groups
 * (per-sstable and per-index), the "complete" (see next paragraph) set of components with the highest version, and
 * highest generation within that version.
 * <p>
 * A group of components may or may not be "complete" ({@link #isComplete()}): it is complete if the completion marker
 * component for that group is present. Groups are temporarily incomplete during writing, but can also be more
 * permanently incomplete for 2 main reasons: a build may fail mid-way, leaving one or more group incomplete, or we can
 * have some corruption of the component files of some sort (corruption could here mean that a file is mistakenly
 * deleted, or that the content of the file is corrupted somehow; the later triggers a removal of the corrupted file and
 * of the completion marker, but not of the other component of the group). The bumping of generations takes incomplete
 * groups into account, and so incomplete groups are not overridden either. Essentially, the generation used by a new
 * build is always one more than the highest generation of any component found on disk (for the group in question, and
 * the version we writting, usually {@link Version#current(String)}).
 */
public interface IndexComponents
{
    Logger logger = LoggerFactory.getLogger(IndexComponents.class);

    /**
     * SSTable this is the group of.
     */
    Descriptor descriptor();

    /**
     * The {@link IndexDescriptor} that created this group.
     * <p>
     * Note that {@link IndexDescriptor} essentially tracks the active and created groups for a sstable, and so a group
     * always comes from a particular {@link IndexDescriptor} instance.
     */
    IndexDescriptor indexDescriptor();

    /**
     * Context of the group.
     *
     * @return the context of the index this is a group of, or {@code null} if the group is a per-sstable group.
     */
    @Nullable IndexContext context();

    /**
     * The build id of the components of this group.
     */
    ComponentsBuildId buildId();

    /**
     * Version used by the components of the groups (part of the {@link #buildId()} but exposed directly because often
     * used).
     */
    default Version version()
    {
        return buildId().version();
    }

    /**
     * The on-disk format used by the these components.
     */
    default OnDiskFormat onDiskFormat()
    {
        return version().onDiskFormat();
    }

    /**
     * Whether that's a per-index group, that is one with components specific to a given index. Otherwise, it is a
     * per-sstable group, that is one with components shared by all the SAI indexes (on that sstable).
     */
    default boolean isPerIndexGroup()
    {
        return context() != null;
    }

    /**
     * The specific component "kind" used for storing the metadata of this group.
     */
    default IndexComponentType metadataComponent()
    {
        return isPerIndexGroup() ? IndexComponentType.META : IndexComponentType.GROUP_META;
    }

    /**
     * The specific component "kind" used as a completion marker for this group.
     */
    default IndexComponentType completionMarkerComponent()
    {
        return isPerIndexGroup() ? IndexComponentType.COLUMN_COMPLETION_MARKER : IndexComponentType.GROUP_COMPLETION_MARKER;
    }

    default String logMessage(String message)
    {
        return indexDescriptor().logMessage(message);
    }

    /**
     * Whether the provided component "kind" exists in this group.
     */
    boolean has(IndexComponentType component);

    /**
     * Whether this group is complete, meaning that is has a completion marker.
     */
    default boolean isComplete()
    {
        return has(completionMarkerComponent());
    }

    /**
     * An empty group is one that is complete, but has only a completion marker and no other components.
     */
    boolean isEmpty();

    /**
     * The complete set of component types that are expected for this group version.
     */
    default Set<IndexComponentType> expectedComponentsForVersion()
    {
        return isPerIndexGroup()
               ? onDiskFormat().perIndexComponentTypes(context())
               : onDiskFormat().perSSTableComponentTypes(hasClustering());
    }

    boolean hasClustering();

    ClusteringComparator comparator();

    default ByteComparable.Version byteComparableVersionFor(IndexComponentType component)
    {
        return version().byteComparableVersionFor(component, descriptor().version);
    }

    /**
     * Specialisation of {@link IndexComponents} used when working with complete and active groups, and so mostly used
     * for reading components.
     */
    interface ForRead extends IndexComponents
    {
        /**
         * Get the component of the provided type if present.
         *
         * @param component the type of the component to retrieve.
         * @return the component, or {@code null} if the group has no such component.
         */
        IndexComponent.ForRead get(IndexComponentType component);

        /**
         * The total size on disk used by the components of this group.
         */
        long liveSizeOnDiskInBytes();

        Collection<IndexComponent.ForRead> all();

        default Set<Component> allAsCustomComponents()
        {
            return all()
                   .stream()
                   .map(IndexComponent::asCustomComponent)
                   .collect(Collectors.toSet());
        }

        /**
         * Validates this group and its components.
         * <p>
         * This is a shortcut for {@link #isValid(boolean, Consumer)} when nothing particular is done for
         * invalid components.
         *
         * @param validateChecksum if {@code true}, the checksum of the components will be validated. Otherwise, only
         *                         basic checks on the header and footers will be performed.
         * @return whether the group is valid.
         */
        default boolean isValid(boolean validateChecksum)
        {
            return isValid(validateChecksum, c -> {});
        }

        /**
         * Validates this group and its components.
         * <p>
         * This method checks that the group has all the components that it should have, and that the content of
         * those components are, as far as this method can determine, valid. Specifics about what failed validation
         * is also be logged.
         *
         * @param validateChecksum if {@code true}, the checksum of the components will be validated. Otherwise, only
         *                         basic checks on the header and footers will be performed.
         * @param onInvalidComponent called on any component that is present but whose content appears invalid/corrupted
         *                           (what is checked depends on {@code validateChecksum}).
         * @return whether the group is valid.
         */
        boolean isValid(boolean validateChecksum, Consumer<IndexComponent> onInvalidComponent);

        /**
         * Validate this group and its components, and invalidate the group if it is invalid.
         * <p>
         * This method is a shortcut for a call to {@link #isValid(boolean, Consumer)} that deletes corrupted components
         * if this is the configured behavior, followed by a call {@link #invalidate} if the validation fails.
         *
         * @param sstable the sstable object for which the component are validated (which should correspond to
         *               {@code this.descriptor()}). This must be provided so if some components are invalid, they
         *               can be unregistered.
         * @param tracker the {@link Tracker} of the table (of the sstable/components). Like the sstable, this is used
         *                as part of unregistering the components when they are invalid.
         * @param validateChecksum if {@code true}, the checksum of the components will be validated. Otherwise, only
         *                         basic checks on the header and footers will be performed.
         * @return whether the group is valid.
         */
        default boolean validateComponents(SSTable sstable, Tracker tracker, boolean validateChecksum)
        {
            boolean isValid = isValid(validateChecksum, invalid -> {
                if (CassandraRelevantProperties.DELETE_CORRUPT_SAI_COMPONENTS.getBoolean())
                {
                    // We delete the corrupted file. Yes, this may break ongoing reads to that component, but
                    // if something is wrong with the file, we're rather fail loudly from that point on than
                    // risking reading and returning corrupted data.
                    forWrite().getForWrite(invalid.componentType()).delete();
                    // Note that invalidation will also delete the completion marker
                }
                else
                {
                    logger.debug("Leaving believed-corrupt component {} of SSTable {} in place because {} is false", invalid.componentType(), descriptor(), CassandraRelevantProperties.DELETE_CORRUPT_SAI_COMPONENTS.getKey());
                }
            });
            if (!isValid)
                invalidate(sstable, tracker);
            return isValid;
        }

        /**
         * Marks the group as invalid/broken.
         * <p>
         * If it is an active group, it will remove it as active in the underlying {@link IndexDescriptor}. It will also
         * at least delete the completion marker to ensure the group does not get used on any reload/restart.
         * <p>
         * Please note that this method is <b>already</b> called by {@link #validateComponents} if the group
         * is found invalid: it is exposed here for case where we have reason to think the group is invalid but
         * validation hasn't detected it for some reason.
         *
         * @param sstable the sstable object for which the component are invalidated (which should correspond to
         *               {@code this.descriptor()}). This must be provided so the invalidated components are
         *               unregistered.
         * @param tracker the {@link Tracker} of the table (of the sstable/components). Like the sstable, this is used
         *                as part of unregistering the components.
         */
        void invalidate(SSTable sstable, Tracker tracker);

        /**
         * Returns a {@link ForWrite} view of this group, mostly for calling {@link ForWrite#forceDeleteAllComponents()} at
         * appropriate times.
         */
        ForWrite forWrite();
    }

    /**
     * Specialisation of {@link IndexComponents} used when doing a new index build and thus writting a new group of
     * components.
     * <p>
     * This extends {@link ForRead} because we sometimes read previously written components to write other ones in the group
     */
    interface ForWrite extends ForRead
    {
        /**
         * Adds the provided component "kind" to this writer, or return the previously added one if it had already
         * been added.
         */
        IndexComponent.ForWrite addOrGet(IndexComponentType component);

        /**
         * Get the component of the provided type if present.
         * <p>
         * This is the same as {@link #get}, except that it preserve the type information that is a "for write" object.
         *
         * @param component the type of the component to retrieve.
         * @return the component, or {@code null} if the group has no such component.
         */
        IndexComponent.ForWrite getForWrite(IndexComponentType component);

        /**
         * Delete the files of all the components in this writer (and remove them so that the group will be empty
         * afterward).
         */
        void forceDeleteAllComponents();

        /**
         * Writes the completion marker file for the group on disk and, if said write succeeds, adds the components
         * of this writer to the {@link IndexDescriptor} that created this writer (and so no additional components
         * should be added to this writer after this call).
         */
        void markComplete() throws IOException;

        /**
         * Create a temporary {@link File} namespaced within the per index components. Repeated calls with the same
         * componentName will produce the same file.
         * @param componentName - unique name within the per index components
         * @return a temprory file for use during index construction
         */
        File tmpFileFor(String componentName) throws IOException;
    }
}
