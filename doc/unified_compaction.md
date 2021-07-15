<!--
#
# Copyright DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
-->

## Unified compaction strategy (UCS)

This is a new compaction strategy that unifies tiered and leveled compaction strategies, adds sharding, lends itself to be reconfigured at any time and forms the basis for future compaction improvements including automatic adaptation to the workload.

The strategy is based on the observation that tiered and levelled compaction can be generalized as the same thing if one observes that both form exponentially-growing levels based on the size of sstables (or non-overlapping sstable runs) and trigger a compaction when more than a given number of sstables are present on one level.

UCS groups sstables in levels based on the logarithm of the sstable size, with
the fanout factor **F** as the base of the logarithm, and with each level triggering a compaction as soon as it has
**T** sstables. The choice of the parameters **F** and **T**, and of a minimum sstable size, determines the behaviour
of the strategy. This allows users to choose a levelled strategy by setting **T=2**, or a tiered strategy by choosing **T=F**. Because the two options are mutually exclusive, meet at **F=2** and form a space of options for choosing different ratios of read amplification (RA) vs write amplification (WA) (where levelled compaction improves reads at the expense of writes and approaches a sorted array as **F** increases, and tiered compaction favors writes at the expense of reads and approaches an unsorted log as **F** increases), we combine the two parameters into one integer value, **W**, and set them to be:
* If **W < 0** then **F = 2 - W** and **T = 2**. This means leveled compactions, high WA but low RA.
* If **W > 0** then set **F = 2 + W** and **T = F**. This means tiered compactions, low WA but high RA.
* If **W = 0** then **F = T = 2**. This is the middle ground, leveled and tiered compactions behave identically.

Further, because levels can choose different values of **W**, levels can behave differently. For example level
zero could behave like STCS but higher levels could behave more and more like LCS.

This strategy also introduces compaction shards. Data is partitioned in independent shards that can be compacted in parallel. Shards are defined by splitting the token ranges for which the node is responsible into equally-sized sections.

## Size based levels

Let's explore more closely how sstables are grouped into levels.

Given:

- the fanout factor **F**
- the sstable flush size **m** (i.e. the average size of sstables written when a memtable is flushed)

then the level **L** for an sstable of size **s** is calculated as follows:

![L = log_F (s/m)](unified_compaction_level_formula.svg)

This means that sstables are assigned to levels as follows:

|Level|Min sstable size|Max sstable size|
|---|---|---|
|0|0|**m&#x2219;F**|
|1|**m&#x2219;F**|**m&#x2219;F<sup>2</sup>**|
|2|**m&#x2219;F<sup>2</sup>**|**m&#x2219;F<sup>3</sup>**|
|3|**m&#x2219;F<sup>3</sup>**|**m&#x2219;F<sup>4</sup>**|
|...|...|...|
|N|**m&#x2219;F<sup>n</sup>**|**m&#x2219;F<sup>n+1</sup>**|

If we define **T** as the number of sstables in a level that triggers a compaction, then:

* **T = 2** means the strategy is using a leveled merged policy. An sstable enters level **n** with size **>=mF<sup>n</sup>**.
  When another sstable enters (also with size **>=mF<sup>n</sup>**) they compact and form a new table with size
  **~2mF<sup>n</sup>**, which keeps the result in the same level for **F > 2**. After this repeats at least **F-2**
  more times (i.e. F tables enter the level altogether), the compaction result grows to **>= mF<sup>n+1</sup>**
  and enters the next level.
* **T = F** means the strategy is using a tiered merge policy. After **F** sstables enter level **n**, each of size **>=mF<sup>n</sup>**, they are compacted together, resulting in an sstable of size **>=mF<sup>n+1</sup>** which belongs to the next level.

Note that the above ignores overwrites and deletions. Given knowledge of the expected proportion of overwrites/deletion, they can also be accounted for (this is not implemented at this time).

For leveled strategies, the write amplification will be proportional to **F-1** times the number of levels whilst
for tiered strategies it will be proportional only to the number of levels. On the other hand, the read
amplification will be proportional to the number of levels for leveled strategies and to **F-1** times the number
of levels for tiered strategies.

The number of levels for our size based scheme can be calculated by substituting the maximal dataset size **D** in our
equation above, giving a maximal number of levels inversely proportional to the logarithm of **F**.

Therefore when we try to control the overheads of compaction on the database, we have a space of choices for the strategy
that range from:

* leveled compaction (**T=2**) with high **F** - low number of levels, high read efficiency, high write cost,
  moving closer to the behaviour of a sorted array as **F** increases;
* compaction with **T = F = 2** where leveled is the same as tiered and we have a middle ground with logarithmically
  increasing read and write costs;
* tiered compaction (**T=F**) with high **F** - very high number of sstables, low read efficiency and low write cost,
  moving closer to an unsorted log as **F** increases.

## Sharding

Sharding is used to reduce the size of the biggest files that are produced by compaction by splitting sstables at selected token boundaries when the data grows above a given size. This helps both with the maximum space overhead that compaction will require, as well as the number of concurrent compactions that can be executed.

The number of required shards is specified in the compaction options. Based on that, the strategy will select shard boundaries which split the token space handled by the node in equal portions. The flush size **m** used in the calculations above is also divided by the number of shards.

When flushing or when compacting, output sstables will be split along the boundaries of compaction shards as long as
they are at least as large as a minimum sstable size specified in the compaction options. If sstables are smaller than this size, then they will continue into the next shard.
The aim is to avoid sstables that are excessively small. For example, if there are four shards
and if the flush size is twice the minimum sstable size, then assuming uniform data distribution (no hot partitions),
flushing will create 2 sstables. The first sstable will be in the first shard and the second sstable will likely be
in the third shard.

This means that some sstables effectively span several shards. We assign such sstables to the shard that contains their start position, but divide the size that we use for the level calculation by the number of spanned shards to reflect the fact that they contain less shard-specific data. This avoids problems with the result of compaction not advancing to the required level because it has shed data belonging to a different shard.

For the example above and a fan factor of 2, the sstables in shards 1 and 3 belong to level 0, because their size is **m * 2** (where **m** is the shard-adjusted flush size), but taking into account that they each span 2 shards, we use the effective size of **m * 2 / 2**. Compacting them with another flushed pair will likely result in 4 sstables of the same size, one in each of the four shards. They, however, will all belong to level 1, as their effective size is **m * 2**.

## Selecting compactions to run

Because of sharding, UCS can do more compactions in parallel. However, it will often not use all available compaction threads.

The reason for this is that UCS splits the available compaction threads equally among the levels of the compaction hierarchy. For example, if there are 16 compaction threads and we have no other work to do but compact the highest level 5, we can only use up to ⌈16/6⌉ = 3 threads to do compactions on level 5. If we permit more, we risk starving the lower levels of resources, which can result in sstables accumulating on level 0 for as long as that compaction takes, which can be a very long time, and breaking our read amplification expectations.

In theory each level requires an equal amount of processing resources: for tiered compaction, every piece of data that enters the system goes through one compaction for each level, and for leveled - through **F-1**. Because of this UCS reserves an equal number of compaction threads for each level, and assign tasks to the remainder of threads randomly.

Make sure the number of compaction threads is greater than the number of expected levels to ensure compaction runs smoothly.

## Differences with STCS and LCS

Note that there are some differences between the tiered flavors of UCS (UCS-tiered) and STCS, and between the leveled flavors of UCS (UCS-leveled) and LCS.

#### UCS-tiered vs STCS

SizeTieredCompactionStrategy is pretty close to UCS. However, it defines buckets/levels by looking for sstables of similar size. This can result in some odd selections of buckets, possibly spanning sstables of wildly different sizes, while UCS's selection is more stable and predictable.

STCS triggers a compaction when it finds at least `min_threshold` sstables on some bucket, and it compacts between `min_threshold` and `max_threshold` sstables from that bucket at a time. `min_threshold` is equivalent to UCS's **T = F = W + 2**. UCS drops the upper limit as we have seen that compaction is still efficient with very large numbers of sstables. 

If there are multiple choices to pick SSTables within a bucket, STCS groups them by size while UCS groups them by timestamp. Because of that, STCS easily loses time order and makes whole table expiration less efficient.

#### UCS-leveled vs LCS

On first glance LeveledCompactionStrategy look very different in behaviour compared to UCS.

LCS keeps multiple sstables per level which form a sorted run of non-overlapping sstables of small fixed size. So physical sstables on increasing levels increase in number (by a factor of `fanout_size`) instead of size. LCS does that to reduce space amplification and to ensure shorter compaction operations. When it finds that the combined size of a run on a level is higher than expected, it selects some sstables to compact with overlapping ones from the next level of the hierarchy. This eventually pushes the size of the next level over its size limit and triggers higher-level operations.

UCS-leveled keeps one sstable per sharded level in the physical sense. So sstables on increasing levels increase in size (by a factor of **F**, see the **Size based levels** section above). UCS-leveled triggers a compaction when it finds a second sstable on some sharded level. It compacts the two sstables on that level, and the result most often ends up on that level too, but eventually it reaches sufficient size for the next level. This is the same time as a run in LCS would outgrow its size, thus compactions are in effect triggered at the same time as LCS would trigger them.

The two approaches end up with a very similar effect, with the added benefit for UCS that sstables are structured in a way that can be easily switched to UCS-tiered or a different set values for the UCS parameters.

UCS deals with the problem of space amplification by sharding on specific token boundaries. LCS's splitting of sstables on a fixed size means that the boundaries usually fall inside sstables on the next level, which tends to cause these sstables to be compacted more often than strictly necessary. This is not acceptable if we need tight write amplification control (i.e. this solution suits UCS-leveled, but not UCS-tiered and is thus not general enough for UCS).

## Configuration

UCS accepts these compaction strategy parameters:

* **static_scaling_parameters**. Typically this will be a single integer, specifying **W** for all levels of the hierarchy. Positive values specify tiered compaction, and negative specify leveled, with fan factor **|W|+2**. Increasing **W** improves write amplification at the expense of reads, and decreasing it improves reads at the expense of writes. The default value is 2, which should be roughly equivalent to using STCS with the default threshold of 4. To use the equivalent of LCS with its default fan factor of 10, set this to -8.<br/>
  The option also accepts passing a list of integers separated by a comma, in which case different values may be passed for the levels of the hierarchy. The first value will be used to set the value of
  **W** for the first level, the second for the second level, and so on. The last value in this list will also be used for
  all remaining levels.

* **num_shards**. This is the number of shards. It is recommended that users set this value. More shards means more parallelism and smaller sstables at the higher levels at the expense of somewhat higher CPU usage.
  By default, 10 would be used for single disk.
  If JBOD / multi-drive, it would be 10 * disks. For example, if there are 5 disks, there would be 50 shards.
  With data size 10 TB, the shard size would be 200 GB, which is an upper bound for the size of the largest sstables and compaction operations.

* **min_sstable_size_in_mb**. This is the minimum sstable size in MB under which data will not be split on shard boundaries, by default 100. Higher values mean fewer sstables on disk and larger compaction operations on the lowest levels of the hierarchy. Storage-attached secondary indexes will work better with higher minimum sstable sizes.

* **dataset_size_in_gb**. This is the target dataset size, by default the minimum total space for all the data file directories.
  This is used to calculate the number of levels and therefore the theoretical read and write amplification.
  It doesn't need to be very accurate but it is recommended
  that it should be set to a value that is close to the target local dataset size, within a few GBs.
  If not given, the database will use the total space on the devices containing the data directories, adjusting for the fact that data is equally split among them.

* **max_space_overhead**. The maximum permitted space overhead as a fraction of the dataset size, by default 0.2 i.e. 20%. This cannot be smaller than 1/num_shards and limits the extra space that is required to complete compactions. UCS will only run compactions that will not overrun this limit. E.g. for datasize of 10TB and 20% max overhead, if a 1.1TB compaction is currently running, it will only start the 1.1TB one in the next shard after it completes. This also means that to prevent running out of space UCS will never start compactions that are larger than the limit by themselves; a warning will be issued if this happens as it may cause performance to deteriorate.

* **expired_sstable_check_frequency_seconds**. Determines how often to check for expired SSTables, 10 minutes by default.

* **unsafe_aggressive_sstable_expiration**. Expired sstables will be dropped without checking if their data is shadowing other sstables, by default false. This flag can only be enabled if `cassandra.allow_unsafe_aggressive_sstable_expiration` is true. Turning this flag can cause correctness issues, e.g. re-appearing of deleted data. See discussions in CASSANDRA-13418, DB-902 for valid use cases and potential problems.

In **cassandra.yaml**:

* **concurrent_compactors**. The number of compaction threads available. Set this to a large number, at minimum the number of expected levels of the compaction hierarchy to make sure that each level is given a dedicated compaction thread. This will avoid latency spikes caused by lower levels of the compaction hierarchy not getting a chance to run.
