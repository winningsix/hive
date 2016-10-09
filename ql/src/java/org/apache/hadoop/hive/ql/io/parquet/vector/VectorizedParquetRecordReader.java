/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.io.parquet.vector;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.parquet.AbstractParquetRecordReader;
import org.apache.hadoop.hive.ql.io.parquet.ProjectionPusher;
import org.apache.hadoop.hive.ql.io.parquet.read.DataWritableReadSupport;
import org.apache.hadoop.hive.ql.io.parquet.serde.ArrayWritableObjectInspector;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridDecoder;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetInputSplit;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;

import static org.apache.parquet.filter2.compat.RowGroupFilter.filterRowGroups;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.range;
import static org.apache.parquet.hadoop.ParquetFileReader.readFooter;
import static org.apache.parquet.hadoop.ParquetInputFormat.getFilter;

public class VectorizedParquetRecordReader extends AbstractParquetRecordReader
  implements RecordReader<NullWritable, VectorizedRowBatch> {
  public static final Logger LOG = LoggerFactory.getLogger(VectorizedParquetRecordReader.class);

  private List<Integer> colsToInclude;

  protected MessageType fileSchema;
  protected MessageType requestedSchema;
  List<String> columnNamesList;
  List<TypeInfo> columnTypesList;

  private VectorizedRowBatchCtx rbCtx;

  /**
   * For each request column, the reader to read this column. This is NULL if this column
   * is missing from the file, in which case we populate the attribute with NULL.
   */
  private VectorizedColumnReader[] columnReaders;

  /**
   * The number of rows that have been returned.
   */
  private long rowsReturned;

  /**
   * The number of rows that have been reading, including the current in flight row group.
   */
  private long totalCountLoadedSoFar = 0;

  /**
   * The total number of rows this RecordReader will eventually read. The sum of the
   * rows of all the row groups.
   */
  protected long totalRowCount;

  public VectorizedParquetRecordReader(
    org.apache.hadoop.mapred.InputSplit oldInputSplit,
    JobConf conf) {
    try {
      serDeStats = new SerDeStats();
      projectionPusher = new ProjectionPusher();
      initialize(oldInputSplit, conf);
      colsToInclude = ColumnProjectionUtils.getReadColumnIDs(conf);
      rbCtx = Utilities.getVectorizedRowBatchCtx(conf);
    } catch (Throwable e) {
      LOG.error("Failed to create the vectorized reader due to exception " + e);
      throw new RuntimeException(e);
    }
  }

  public VectorizedParquetRecordReader(
    InputSplit inputSplit,
    JobConf conf) {
    try {
      serDeStats = new SerDeStats();
      projectionPusher = new ProjectionPusher();
      initialize(inputSplit, conf);
      colsToInclude = ColumnProjectionUtils.getReadColumnIDs(conf);
      rbCtx = new VectorizedRowBatchCtx();
      rbCtx.init(createStructObjectInspector(), new String[0]);
    } catch (Throwable e) {
      LOG.error("Failed to create the vectorized reader due to exception " + e);
      throw new RuntimeException(e);
    }
  }

  private StructObjectInspector createStructObjectInspector() {
    // Create row related objects
    TypeInfo rowTypeInfo = TypeInfoFactory.getStructTypeInfo(columnNamesList, columnTypesList);
    return new ArrayWritableObjectInspector((StructTypeInfo) rowTypeInfo);
  }


  public void initialize(
    org.apache.hadoop.mapred.InputSplit oldInputSplit,
    JobConf configuration) throws IOException, InterruptedException {
    initialize(getSplit(oldInputSplit, configuration), configuration);
  }

  public void initialize(
    InputSplit oldSplit,
    JobConf configuration) throws IOException, InterruptedException {
    jobConf = configuration;
    ParquetMetadata footer;
    List<BlockMetaData> blocks;
    ParquetInputSplit split = (ParquetInputSplit) oldSplit;
    boolean indexAccess =
      configuration.getBoolean(DataWritableReadSupport.PARQUET_COLUMN_INDEX_ACCESS, false);
    this.file = split.getPath();
    long[] rowGroupOffsets = split.getRowGroupOffsets();

    String columnNames = configuration.get(IOConstants.COLUMNS);
    columnNamesList = DataWritableReadSupport.getColumnNames(columnNames);
    String columnTypes = configuration.get(IOConstants.COLUMNS_TYPES);
    columnTypesList = DataWritableReadSupport.getColumnTypes(columnTypes);

    // if task.side.metadata is set, rowGroupOffsets is null
    if (rowGroupOffsets == null) {
      // then we need to apply the predicate push down filter
      footer = readFooter(configuration, file, range(split.getStart(), split.getEnd()));
      MessageType fileSchema = footer.getFileMetaData().getSchema();
      FilterCompat.Filter filter = getFilter(configuration);
      blocks = filterRowGroups(filter, footer.getBlocks(), fileSchema);
    } else {
      // otherwise we find the row groups that were selected on the client
      footer = readFooter(configuration, file, NO_FILTER);
      Set<Long> offsets = new HashSet<>();
      for (long offset : rowGroupOffsets) {
        offsets.add(offset);
      }
      blocks = new ArrayList<>();
      for (BlockMetaData block : footer.getBlocks()) {
        if (offsets.contains(block.getStartingPos())) {
          blocks.add(block);
        }
      }
      // verify we found them all
      if (blocks.size() != rowGroupOffsets.length) {
        long[] foundRowGroupOffsets = new long[footer.getBlocks().size()];
        for (int i = 0; i < foundRowGroupOffsets.length; i++) {
          foundRowGroupOffsets[i] = footer.getBlocks().get(i).getStartingPos();
        }
        // this should never happen.
        // provide a good error message in case there's a bug
        throw new IllegalStateException(
          "All the offsets listed in the split should be found in the file."
            + " expected: " + Arrays.toString(rowGroupOffsets)
            + " found: " + blocks
            + " out of: " + Arrays.toString(foundRowGroupOffsets)
            + " in range " + split.getStart() + ", " + split.getEnd());
      }
    }

    for (BlockMetaData block : blocks) {
      this.totalRowCount += block.getRowCount();
    }
    this.fileSchema = footer.getFileMetaData().getSchema();


    MessageType tableSchema;
    if (indexAccess) {
      List<Integer> indexSequence = new ArrayList<Integer>();

      // Generates a sequence list of indexes
      for(int i = 0; i < columnNamesList.size(); i++) {
        indexSequence.add(i);
      }

      tableSchema = getSchemaByIndex(fileSchema, columnNamesList, indexSequence);
    } else {

      tableSchema = getSchemaByName(fileSchema, columnNamesList, columnTypesList);
    }
//    this.hiveTypeInfo = TypeInfoFactory.getStructTypeInfo(columnNamesList, columnTypesList);

    List<Integer> indexColumnsWanted = ColumnProjectionUtils.getReadColumnIDs(configuration);
    if (!ColumnProjectionUtils.isReadAllColumns(configuration) && !indexColumnsWanted.isEmpty()) {
      requestedSchema = getSchemaByIndex(tableSchema, columnNamesList, indexColumnsWanted);
    }else{
      requestedSchema = fileSchema;
    }

    this.reader = new ParquetFileReader(
      configuration, footer.getFileMetaData(), file, blocks, requestedSchema.getColumns());
  }

  /**
   * Searchs column names by name on a given Parquet schema, and returns its corresponded
   * Parquet schema types.
   *
   * @param schema Group schema where to search for column names.
   * @param colNames List of column names.
   * @param colTypes List of column types.
   * @return List of GroupType objects of projected columns.
   */
  private static List<Type> getProjectedGroupFields(GroupType schema, List<String> colNames, List<TypeInfo> colTypes) {
    List<Type> schemaTypes = new ArrayList<Type>();

    ListIterator<String> columnIterator = colNames.listIterator();
    while (columnIterator.hasNext()) {
      TypeInfo colType = colTypes.get(columnIterator.nextIndex());
      String colName = columnIterator.next();

      Type fieldType = getFieldTypeIgnoreCase(schema, colName);
      if (fieldType == null) {
        schemaTypes.add(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).named(colName));
      } else {
        schemaTypes.add(getProjectedType(colType, fieldType));
      }
    }

    return schemaTypes;
  }

  private static Type getProjectedType(TypeInfo colType, Type fieldType) {
    switch (colType.getCategory()) {
    case STRUCT:
      List<Type> groupFields = getProjectedGroupFields(
        fieldType.asGroupType(),
        ((StructTypeInfo) colType).getAllStructFieldNames(),
        ((StructTypeInfo) colType).getAllStructFieldTypeInfos()
      );

      Type[] typesArray = groupFields.toArray(new Type[0]);
      return Types.buildGroup(fieldType.getRepetition())
        .addFields(typesArray)
        .named(fieldType.getName());
    case LIST:
      TypeInfo elemType = ((ListTypeInfo) colType).getListElementTypeInfo();
      if (elemType.getCategory() == ObjectInspector.Category.STRUCT) {
        Type subFieldType = fieldType.asGroupType().getType(0);
        if (!subFieldType.isPrimitive()) {
          String subFieldName = subFieldType.getName();
          Text name = new Text(subFieldName);
          if (name.equals(ParquetHiveSerDe.ARRAY) || name.equals(ParquetHiveSerDe.LIST)) {
            subFieldType = new GroupType(Type.Repetition.REPEATED, subFieldName,
              getProjectedType(elemType, subFieldType.asGroupType().getType(0)));
          } else {
            subFieldType = getProjectedType(elemType, subFieldType);
          }
          return Types.buildGroup(Type.Repetition.OPTIONAL).as(OriginalType.LIST).addFields(
            subFieldType).named(fieldType.getName());
        }
      }
      break;
    default:
    }
    return fieldType;
  }


  /**
   * Searchs for a fieldName into a parquet GroupType by ignoring string case.
   * GroupType#getType(String fieldName) is case sensitive, so we use this method.
   *
   * @param groupType Group of field types where to search for fieldName
   * @param fieldName The field what we are searching
   * @return The Type object of the field found; null otherwise.
   */
  private static Type getFieldTypeIgnoreCase(GroupType groupType, String fieldName) {
    for (Type type : groupType.getFields()) {
      if (type.getName().equalsIgnoreCase(fieldName)) {
        return type;
      }
    }

    return null;
  }


  /**
   * Searchs column names by name on a given Parquet message schema, and returns its projected
   * Parquet schema types.
   *
   * @param schema Message type schema where to search for column names.
   * @param colNames List of column names.
   * @param colTypes List of column types.
   * @return A MessageType object of projected columns.
   */
  private static MessageType getSchemaByName(MessageType schema, List<String> colNames, List<TypeInfo> colTypes) {
    List<Type> projectedFields = getProjectedGroupFields(schema, colNames, colTypes);
    Type[] typesArray = projectedFields.toArray(new Type[0]);

    return Types.buildMessage()
      .addFields(typesArray)
      .named(schema.getName());
  }

  /**
   * Searchs column names by index on a given Parquet file schema, and returns its corresponded
   * Parquet schema types.
   *
   * @param schema Message schema where to search for column names.
   * @param colNames List of column names.
   * @param colIndexes List of column indexes.
   * @return A MessageType object of the column names found.
   */
  private static MessageType getSchemaByIndex(MessageType schema, List<String> colNames, List<Integer> colIndexes) {
    List<Type> schemaTypes = new ArrayList<Type>();

    for (Integer i : colIndexes) {
      if (i < colNames.size()) {
        if (i < schema.getFieldCount()) {
          schemaTypes.add(schema.getType(i));
        } else {
          //prefixing with '_mask_' to ensure no conflict with named
          //columns in the file schema
          schemaTypes.add(Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).named("_mask_" + colNames.get(i)));
        }
      }
    }
    return new MessageType(schema.getName(), schemaTypes);
  }

  /**
   * columnBatch object that is used for batch decoding. This is created on first use and triggers
   * batched decoding. It is not valid to interleave calls to the batched interface with the row
   * by row RecordReader APIs.
   * This is only enabled with additional flags for development. This is still a work in progress
   * and currently unsupported cases will fail with potentially difficult to diagnose errors.
   * This should be only turned on for development to work on this feature.
   *
   * When this is set, the code will branch early on in the RecordReader APIs. There is no shared
   * code between the path that uses the MR decoders and the vectorized ones.
   *
   * TODOs:
   *  - Implement v2 page formats (just make sure we create the correct decoders).
   */
  private VectorizedRowBatch columnarBatch;

  @Override
  public boolean next(
    NullWritable nullWritable,
    VectorizedRowBatch vectorizedRowBatch) throws IOException {
    columnarBatch = vectorizedRowBatch;
    return nextBatch();
  }

  /**
   * Advances to the next batch of rows. Returns false if there are no more.
   */
  private boolean nextBatch() throws IOException {
    initRowBatch();
    columnarBatch.reset();
    if (rowsReturned >= totalRowCount) {
      return false;
    }
    checkEndOfRowGroup();

    int num = (int) Math.min(VectorizedRowBatch.DEFAULT_SIZE, totalCountLoadedSoFar - rowsReturned);
    for (int i = 0; i < columnReaders.length; ++i) {
      if (columnReaders[i] == null) {
        continue;
      }
      columnarBatch.cols[colsToInclude.get(i)].isRepeating = true;
      columnReaders[i].readBatch(num, columnarBatch.cols[colsToInclude.get(i)],
        columnTypesList.get(colsToInclude.get(i)));
    }
    rowsReturned += num;
    columnarBatch.size = num;
    return true;
  }

  private void initRowBatch() {
    if (columnarBatch == null) {
      if (rbCtx != null) {
        columnarBatch = rbCtx.createVectorizedRowBatch();
      } else {
        // test only
        rbCtx = new VectorizedRowBatchCtx();
        try {
          rbCtx.init(createStructObjectInspector(), new String[0]);
        } catch (HiveException e) {
          e.printStackTrace();
        }
      }
    }
  }

  private void checkEndOfRowGroup() throws IOException {
    if (rowsReturned != totalCountLoadedSoFar) return;
    PageReadStore pages = reader.readNextRowGroup();
    if (pages == null) {
      throw new IOException("expecting more rows but reached last block. Read "
        + rowsReturned + " out of " + totalRowCount);
    }
    List<ColumnDescriptor> columns = requestedSchema.getColumns();
    List<Type> types = requestedSchema.getFields();
    columnReaders = new VectorizedColumnReader[columns.size()];
    for (int i = 0; i < columns.size(); ++i) {
      columnReaders[i] =
        new VectorizedColumnReader(columns.get(i), pages.getPageReader(columns.get(i)),
          skipTimestampConversion, types.get(i));
    }
    totalCountLoadedSoFar += pages.getRowCount();
  }

  @Override
  public NullWritable createKey() {
    return NullWritable.get();
  }

  @Override
  public VectorizedRowBatch createValue() {
    initRowBatch();
    return columnarBatch;
  }

  @Override
  public long getPos() throws IOException {
    //TODO
    return 0;
  }

  @Override
  public void close() throws IOException {
    if (columnarBatch != null) {
      columnarBatch = null;
    }
  }

  @Override
  public float getProgress() throws IOException {
    //TODO
    return 0;
  }
}
