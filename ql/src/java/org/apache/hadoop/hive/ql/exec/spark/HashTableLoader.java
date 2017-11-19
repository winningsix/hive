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
package org.apache.hadoop.hive.ql.exec.spark;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.lang.management.ManagementFactory;
import java.util.*;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hive.ql.exec.mapjoin.MapJoinMemoryExhaustionError;
import org.apache.hadoop.hive.ql.exec.persistence.*;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.fast.VectorMapJoinFastTableContainer;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.HashTableSinkOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TemporaryHashSinkOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapperContext;
import org.apache.hadoop.hive.ql.exec.mr.MapredLocalTask;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationOperator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.BucketMapJoinContext;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.MapredLocalWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.SparkBucketMapJoinContext;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.mapred.JobConf;

/**
 * HashTableLoader for Spark to load the hashtable for MapJoins.
 */
public class HashTableLoader implements org.apache.hadoop.hive.ql.exec.HashTableLoader {

  private static final Logger LOG = LoggerFactory.getLogger(HashTableLoader.class.getName());

  private ExecMapperContext context;
  private Configuration hconf;

  private MapJoinOperator joinOp;
  private MapJoinDesc desc;

  private boolean useFastContainer = false;

  @Override
  public void init(ExecMapperContext context, MapredContext mrContext, Configuration hconf,
      MapJoinOperator joinOp) {
    this.context = context;
    this.hconf = hconf;
    this.joinOp = joinOp;
    this.desc = joinOp.getConf();
    if (desc.getVectorMode() && HiveConf.getBoolVar(
        hconf, HiveConf.ConfVars.HIVE_VECTORIZATION_MAPJOIN_NATIVE_FAST_HASHTABLE_ENABLED)) {
      if (joinOp instanceof VectorizationOperator) {
        VectorMapJoinDesc vectorDesc = (VectorMapJoinDesc) ((VectorizationOperator) joinOp).getVectorDesc();
        useFastContainer = vectorDesc != null && vectorDesc.getHashTableImplementationType() ==
            VectorMapJoinDesc.HashTableImplementationType.FAST;
      }
    }
  }

  @Override
  public void load(MapJoinTableContainer[] mapJoinTables,
      MapJoinTableContainerSerDe[] mapJoinTableSerdes)
      throws HiveException {

    // Note: it's possible that a MJ operator is in a ReduceWork, in which case the
    // currentInputPath will be null. But, since currentInputPath is only interesting
    // for bucket join case, and for bucket join the MJ operator will always be in
    // a MapWork, this should be OK.
    String currentInputPath =
        context.getCurrentInputPath() == null ? null : context.getCurrentInputPath().toString();

    LOG.info("******* Load from HashTable for input file: " + currentInputPath);

    MapredLocalWork localWork = context.getLocalWork();
    try {
      if (localWork.getDirectFetchOp() != null) {
        loadDirectly(mapJoinTables, currentInputPath);
      }
      // All HashTables share the same base dir,
      // which is passed in as the tmp path
      Path baseDir = localWork.getTmpPath();
      if (baseDir == null) {
        return;
      }
      FileSystem fs = FileSystem.get(baseDir.toUri(), hconf);
      BucketMapJoinContext mapJoinCtx = localWork.getBucketMapjoinContext();
      boolean firstContainer = true;
      boolean useOptimizedContainer = !useFastContainer && HiveConf.getBoolVar(
          hconf, HiveConf.ConfVars.HIVEMAPJOINUSEOPTIMIZEDTABLE);
      boolean useHybridGraceHashJoin = desc.isHybridHashJoin();

      long totalMapJoinMemory = 0;

      Map<Integer, Long> parentKeyCounts = desc.getParentKeyCounts();

      HybridHashTableConf nwayConf = null;
      Map<Integer, Long> tableMemorySizes = null;
      if (useHybridGraceHashJoin) {
        totalMapJoinMemory = getTotalMapJoinMemory();
        if (mapJoinTables.length > 2) {
          // Create a Conf for n-way HybridHashTableContainers
          nwayConf = new HybridHashTableConf();
          LOG.info("N-way join: " + (mapJoinTables.length - 1) + " small tables.");

          // Find the biggest small table; also calculate total data size of all small tables
          long maxSize = Long.MIN_VALUE; // the size of the biggest small table
          long totalSize = 0;
          int biggest = 0;
          for (int pos = 0; pos < mapJoinTables.length; pos++) {
            if (pos == desc.getPosBigTable()) {
              continue;
            }
            long smallTableSize = desc.getParentDataSizes().get(pos);
            totalSize += smallTableSize;
            if (maxSize < smallTableSize) {
              maxSize = smallTableSize;
              biggest = pos;
            }
          }

          tableMemorySizes = divideHybridHashTableMemory(mapJoinTables, desc,
                  totalSize, totalMapJoinMemory);

          // Using biggest small table, calculate number of partitions to create for each small table
          long memory = tableMemorySizes.get(biggest);
          int numPartitions;
          try {
            numPartitions = HybridHashTableContainer.calcNumPartitions(memory, maxSize,
                    HiveConf.getIntVar(hconf, HiveConf.ConfVars.HIVEHYBRIDGRACEHASHJOINMINNUMPARTITIONS),
                    HiveConf.getIntVar(hconf, HiveConf.ConfVars.HIVEHYBRIDGRACEHASHJOINMINWBSIZE));
          } catch (IOException e) {
            throw new HiveException(e);
          }
          nwayConf.setNumberOfPartitions(numPartitions);
        }
      }


      for (int pos = 0; pos < mapJoinTables.length; pos++) {
        if (pos == desc.getPosBigTable() || mapJoinTables[pos] != null) {
          continue;
        }

        if (useOptimizedContainer) {
          if (useHybridGraceHashJoin) {

            Long keyCountObj = parentKeyCounts.get(pos);
            long keyCount = (keyCountObj == null) ? -1 : keyCountObj.longValue();

            long memory = 0;
            if (useHybridGraceHashJoin) {
              if (mapJoinTables.length > 2) {
                memory = tableMemorySizes.get(pos);
              } else {  // binary join
                memory = totalMapJoinMemory;
              }
            }

            mapJoinTables[pos] = new HybridHashTableContainer(hconf, keyCount, memory,
                    desc.getParentDataSizes().get(pos), nwayConf);
          }
          MapJoinObjectSerDeContext keyCtx = mapJoinTableSerdes[pos].getKeyContext();

          ObjectInspector keyOI = keyCtx.getSerDe().getObjectInspector();
          if (!MapJoinBytesTableContainer.isSupportedKey(keyOI)) {
            if (firstContainer) {
              LOG.warn("Not using optimized table container." +
                "Only a subset of mapjoin keys is supported.");
              useOptimizedContainer = false;
              HiveConf.setBoolVar(hconf, HiveConf.ConfVars.HIVEMAPJOINUSEOPTIMIZEDTABLE, false);
            } else {
              throw new HiveException("Only a subset of mapjoin keys is supported.");
            }
          }
        }
        firstContainer = false;
        String bigInputPath = currentInputPath;
        if (currentInputPath != null && mapJoinCtx != null) {
          if (!desc.isBucketMapJoin()) {
            bigInputPath = null;
          } else {
            Set<String> aliases =
              ((SparkBucketMapJoinContext) mapJoinCtx).getPosToAliasMap().get(pos);
            String alias = aliases.iterator().next();
            // Any one small table input path
            String smallInputPath =
              mapJoinCtx.getAliasBucketFileNameMapping().get(alias).get(bigInputPath).get(0);
            bigInputPath = mapJoinCtx.getMappingBigFile(alias, smallInputPath);
          }
        }
        String fileName = localWork.getBucketFileName(bigInputPath);
        Path path = Utilities.generatePath(baseDir, desc.getDumpFilePrefix(), (byte) pos, fileName);
        if (useHybridGraceHashJoin) {
          loadHybridGraceHashJoin(fs, path, (HybridHashTableContainer) mapJoinTables[pos], mapJoinTableSerdes[pos]);
        } else {
          mapJoinTables[pos] = load(fs, path, mapJoinTableSerdes[pos]);
        }
      }
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  //FIXME Does cache work for hybrid grace hash join for Spark path?
  private MapJoinTableContainer loadHybridGraceHashJoin(FileSystem fs, Path path, HybridHashTableContainer hashJoinContainer, MapJoinTableContainerSerDe mapJoinTableSerde) throws HiveException {
    LOG.info("\tLoad back all hashtable files from tmp folder uri:" + path);
    if (!SparkUtilities.isDedicatedCluster(hconf)) {
      if (useFastContainer) {
        mapJoinTableSerde.loadFastHybridHashJoinContainer(fs, path, hashJoinContainer);
      } else {
        mapJoinTableSerde.load(fs, path, hconf);
      }
    }
    MapJoinTableContainer mapJoinTable = SmallTableCache.get(path);
    if (mapJoinTable == null) {
      synchronized (path.toString().intern()) {
        mapJoinTable = SmallTableCache.get(path);
        if (mapJoinTable == null) {
          if (useFastContainer) {
            mapJoinTableSerde.loadFastHybridHashJoinContainer(fs, path, hashJoinContainer);
          } else {
            mapJoinTableSerde.load(fs, path, hconf);
          }
          SmallTableCache.cache(path, mapJoinTable);
        }
      }
    }
    return mapJoinTable;
  }

  private MapJoinTableContainer load(FileSystem fs, Path path,
      MapJoinTableContainerSerDe mapJoinTableSerde) throws HiveException {
    LOG.info("\tLoad back all hashtable files from tmp folder uri:" + path);
    if (!SparkUtilities.isDedicatedCluster(hconf)) {
      return useFastContainer ? mapJoinTableSerde.loadFastContainer(desc, fs, path, hconf) :
          mapJoinTableSerde.load(fs, path, hconf);
    }
    MapJoinTableContainer mapJoinTable = SmallTableCache.get(path);
    if (mapJoinTable == null) {
      synchronized (path.toString().intern()) {
        mapJoinTable = SmallTableCache.get(path);
        if (mapJoinTable == null) {
          mapJoinTable = useFastContainer ?
              mapJoinTableSerde.loadFastContainer(desc, fs, path, hconf) :
              mapJoinTableSerde.load(fs, path, hconf);
          SmallTableCache.cache(path, mapJoinTable);
        }
      }
    }
    return mapJoinTable;
  }

  private void loadDirectly(MapJoinTableContainer[] mapJoinTables, String inputFileName)
      throws Exception {
    MapredLocalWork localWork = context.getLocalWork();
    List<Operator<?>> directWorks = localWork.getDirectFetchOp().get(joinOp);
    if (directWorks == null || directWorks.isEmpty()) {
      return;
    }
    JobConf job = new JobConf(hconf);
    MapredLocalTask localTask = new MapredLocalTask(localWork, job, false);

    HashTableSinkOperator sink = new TemporaryHashSinkOperator(new CompilationOpContext(), desc);
    sink.setParentOperators(new ArrayList<Operator<? extends OperatorDesc>>(directWorks));

    for (Operator<?> operator : directWorks) {
      if (operator != null) {
        operator.setChildOperators(Arrays.<Operator<? extends OperatorDesc>>asList(sink));
      }
    }
    localTask.setExecContext(context);
    localTask.startForward(inputFileName);

    MapJoinTableContainer[] tables = sink.getMapJoinTables();
    for (int i = 0; i < sink.getNumParent(); i++) {
      if (sink.getParentOperators().get(i) != null) {
        mapJoinTables[i] = tables[i];
      }
    }

    Arrays.fill(tables, null);
  }

  // Used for hybrid grace hash join. The code is referring from tez/HashTableLoader.
  private long getTotalMapJoinMemory(){
    // Get the total available memory from memory manager, code is referring from Tez/hashTable
    long totalMapJoinMemory = desc.getMemoryNeeded();
    LOG.info("Memory manager allocates " + totalMapJoinMemory + " bytes for the loading hashtable.");
    if (totalMapJoinMemory <= 0) {
      totalMapJoinMemory = HiveConf.getLongVar(
              hconf, HiveConf.ConfVars.HIVECONVERTJOINNOCONDITIONALTASKTHRESHOLD);
    }

    long processMaxMemory = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax();
    if (totalMapJoinMemory > processMaxMemory) {
      float hashtableMemoryUsage = HiveConf.getFloatVar(
              hconf, HiveConf.ConfVars.HIVEHASHTABLEFOLLOWBYGBYMAXMEMORYUSAGE);
      LOG.warn("totalMapJoinMemory value of " + totalMapJoinMemory +
              " is greater than the max memory size of " + processMaxMemory);
      // Don't want to attempt to grab more memory than we have available .. percentage is a bit arbitrary
      totalMapJoinMemory = (long) (processMaxMemory * hashtableMemoryUsage);
    }
    return totalMapJoinMemory;
  }

  private static Map<Integer, Long> divideHybridHashTableMemory(
          MapJoinTableContainer[] mapJoinTables, MapJoinDesc desc,
          long totalSize, long totalHashTableMemory) {
    int smallTableCount = Math.max(mapJoinTables.length - 1, 1);
    Map<Integer, Long> tableMemorySizes = new HashMap<Integer, Long>();
    // If any table has bad size estimate, we need to fall back to sizing each table equally
    boolean fallbackToEqualProportions = totalSize <= 0;

    if (!fallbackToEqualProportions) {
      for (Map.Entry<Integer, Long> tableSizeEntry : desc.getParentDataSizes().entrySet()) {
        if (tableSizeEntry.getKey() == desc.getPosBigTable()) {
          continue;
        }

        long tableSize = tableSizeEntry.getValue();
        if (tableSize <= 0) {
          fallbackToEqualProportions = true;
          break;
        }
        float percentage = (float) tableSize / totalSize;
        long tableMemory = (long) (totalHashTableMemory * percentage);
        tableMemorySizes.put(tableSizeEntry.getKey(), tableMemory);
      }
    }

    if (fallbackToEqualProportions) {
      // Just give each table the same amount of memory.
      long equalPortion = totalHashTableMemory / smallTableCount;
      for (Integer pos : desc.getParentDataSizes().keySet()) {
        if (pos == desc.getPosBigTable()) {
          break;
        }
        tableMemorySizes.put(pos, equalPortion);
      }
    }

    return tableMemorySizes;
  }
}
