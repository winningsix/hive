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

import jodd.datetime.JDateTime;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridDecoder;
import org.apache.parquet.example.data.simple.NanoTime;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.Type;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import static org.apache.parquet.column.ValuesType.DEFINITION_LEVEL;
import static org.apache.parquet.column.ValuesType.REPETITION_LEVEL;

public class VectorizedColumnReader {
  static final long NANOS_PER_HOUR = TimeUnit.HOURS.toNanos(1);
  static final long NANOS_PER_MINUTE = TimeUnit.MINUTES.toNanos(1);
  static final long NANOS_PER_SECOND = TimeUnit.SECONDS.toNanos(1);
  static final long NANOS_PER_DAY = TimeUnit.DAYS.toNanos(1);
  private boolean skipTimestampConversion = false;
  private static final ThreadLocal<Calendar> parquetGMTCalendar = new ThreadLocal<>();
  private static final ThreadLocal<Calendar> parquetLocalCalendar = new ThreadLocal<>();

  /**
   * Total number of values read.
   */
  private long valuesRead;

  /**
   * value that indicates the end of the current page. That is,
   * if valuesRead == endOfPageValueCount, we are at the end of the page.
   */
  private long endOfPageValueCount;

  /**
   * The dictionary, if this column has dictionary encoding.
   */
  private final Dictionary dictionary;

  /**
   * If true, the current page is dictionary encoded.
   */
  private boolean isCurrentPageDictionaryEncoded;

  /**
   * Maximum definition level for this column.
   */
  private final int maxDefLevel;

  private int definitionLevel;
  private int repetitionLevel;

  /**
   * Repetition/Definition/Value readers.
   */
  private VectorizedParquetRecordReader.IntIterator repetitionLevelColumn;
  private VectorizedParquetRecordReader.IntIterator definitionLevelColumn;
  private ValuesReader dataColumn;

  // Only set if vectorized decoding is true. This is used instead of the row by row decoding
  // with `definitionLevelColumn`.
  private VectorizedRleValuesReader defColumn;

  /**
   * Total number of values in this column (in this row group).
   */
  private final long totalValueCount;

  /**
   * Total values in the current page.
   */
  private int pageValueCount;

  private final PageReader pageReader;
  private final ColumnDescriptor descriptor;
  private final Type type;

  public VectorizedColumnReader(
    ColumnDescriptor descriptor,
    PageReader pageReader,
    boolean skipTimestampConversion,
    Type type) throws IOException {
    this.descriptor = descriptor;
    this.type = type;
    this.pageReader = pageReader;
    this.maxDefLevel = descriptor.getMaxDefinitionLevel();
    this.skipTimestampConversion = skipTimestampConversion;

    DictionaryPage dictionaryPage = pageReader.readDictionaryPage();
    if (dictionaryPage != null) {
      try {
        this.dictionary = dictionaryPage.getEncoding().initDictionary(descriptor, dictionaryPage);
        this.isCurrentPageDictionaryEncoded = true;
      } catch (IOException e) {
        throw new IOException("could not decode the dictionary for " + descriptor, e);
      }
    } else {
      this.dictionary = null;
      this.isCurrentPageDictionaryEncoded = false;
    }
    this.totalValueCount = pageReader.getTotalValueCount();
    if (totalValueCount == 0) {
      throw new IOException("totalValueCount == 0");
    }
  }

  void readBatch(
    int total,
    ColumnVector column,
    TypeInfo columnType) throws IOException {

    int rowId = 0;
    while (total > 0) {
      // Compute the number of values we want to read in this page.
      consume();
      int leftInPage = (int) (endOfPageValueCount - valuesRead);
      int num = Math.min(total, leftInPage);
      if (isCurrentPageDictionaryEncoded) {
        LongColumnVector dictionaryIds = new LongColumnVector();
        // Read and decode dictionary ids.
        readIntegers(num, dictionaryIds, rowId, maxDefLevel,
          (VectorizedValuesReader) dataColumn);
        decodeDictionaryIds(rowId, num, column, dictionaryIds);
      } else {
        // assign values in vector
        PrimitiveTypeInfo primitiveColumnType = (PrimitiveTypeInfo) columnType;
        switch (primitiveColumnType.getPrimitiveCategory()) {
        case INT:
        case BYTE:
        case SHORT:
          readIntegers(num, (LongColumnVector) column, rowId, maxDefLevel,
            (VectorizedValuesReader) dataColumn);
          break;
        case DATE:
        case INTERVAL_YEAR_MONTH:
        case LONG:
          readLongs(num, (LongColumnVector) column, rowId, maxDefLevel,
            (VectorizedValuesReader) dataColumn);
          break;
        case BOOLEAN:
          readBooleans(num, (LongColumnVector) column, rowId, maxDefLevel,
            (VectorizedValuesReader) dataColumn);
          break;
        case DOUBLE:
          readDoubles(num, (DoubleColumnVector) column, rowId, maxDefLevel,
            (VectorizedValuesReader) dataColumn);
          break;
        case BINARY:
        case STRING:
        case CHAR:
        case VARCHAR:
          readBinarys(num, (BytesColumnVector) column, rowId, maxDefLevel,
            (VectorizedValuesReader) dataColumn);
          break;
        case FLOAT:
          readFloats(num, (DoubleColumnVector) column, rowId, maxDefLevel,
            (VectorizedValuesReader) dataColumn);
          break;
        case DECIMAL:
          readDecimal(num, (DecimalColumnVector) column, rowId, maxDefLevel,
            (VectorizedValuesReader) dataColumn);
        case INTERVAL_DAY_TIME:
        case TIMESTAMP:
          break;
        default:
          throw new IOException("Unsupported");
        }
      }
      rowId += num;
      total -= num;
    }
  }

  private void consume() throws IOException {
    int leftInPage = (int) (endOfPageValueCount - valuesRead);
    if (leftInPage == 0) {
      readPage();
    }
  }

  public void readIntegers(
    int total,
    LongColumnVector c,
    int rowId,
    int level,
    VectorizedValuesReader data) throws IOException {
    int left = total;
    while (left > 0) {
      consume();
      readRepetitionAndDefinitionLevels();
      if (definitionLevel >= level) {
        c.vector[rowId] = data.readInteger();
        c.isNull[rowId] = false;
        if(c.isRepeating){
          c.isRepeating = (c.vector[0] == c.vector[rowId]);
        }
      } else {
        c.isNull[rowId] = true;
        c.isRepeating = false;
        c.noNulls = false;
      }
      rowId++;
      left--;
    }
  }

  public void readDoubles(
    int total,
    DoubleColumnVector c,
    int rowId,
    int level,
    VectorizedValuesReader data) throws IOException {
    int left = total;
    while (left > 0) {
      consume();
      readRepetitionAndDefinitionLevels();
      if (definitionLevel >= level) {
        c.vector[rowId] = data.readDouble();
        c.isNull[rowId] = false;
        if(c.isRepeating){
          c.isRepeating = (c.vector[0] == c.vector[rowId]);
        }
      } else {
        c.isNull[rowId] = true;
        c.isRepeating = false;
        c.noNulls = false;
      }
      rowId++;
      left--;
    }
  }

  public void readBooleans(
    int total,
    LongColumnVector c,
    int rowId,
    int level,
    VectorizedValuesReader data) throws IOException {
    int left = total;
    while (left > 0) {
      consume();
      readRepetitionAndDefinitionLevels();
      if (definitionLevel >= level) {
        c.vector[rowId] = data.readBoolean() ? 1 : 0;
        c.isNull[rowId] = false;
        if(c.isRepeating){
          c.isRepeating = (c.vector[0] == c.vector[rowId]);
        }
      } else {
        c.isNull[rowId] = true;
        c.isRepeating = false;
        c.noNulls = false;
      }
      rowId++;
      left--;
    }
  }

  public void readLongs(
    int total,
    LongColumnVector c,
    int rowId,
    int level,
    VectorizedValuesReader data) throws IOException {
    int left = total;
    while (left > 0) {
      consume();
      readRepetitionAndDefinitionLevels();
      if (definitionLevel >= level) {
        c.vector[rowId] = data.readLong();
        c.isNull[rowId] = false;
        if(c.isRepeating){
          c.isRepeating = (c.vector[0] == c.vector[rowId]);
        }
      } else {
        c.isNull[rowId] = true;
        c.isRepeating = false;
        c.noNulls = false;
      }
      rowId++;
      left--;
    }
  }

  public void readFloats(
    int total,
    DoubleColumnVector c,
    int rowId,
    int level,
    VectorizedValuesReader data) throws IOException {
    int left = total;
    while (left > 0) {
      consume();
      readRepetitionAndDefinitionLevels();
      if (definitionLevel >= level) {
        c.vector[rowId] = data.readFloat();
        c.isNull[rowId] = false;
        if(c.isRepeating){
          c.isRepeating = (c.vector[0] == c.vector[rowId]);
        }
      } else {
        c.isNull[rowId] = true;
        c.isRepeating = false;
        c.noNulls = false;
      }
      rowId++;
      left--;
    }
  }

  public void readDecimal(
    int total,
    DecimalColumnVector c,
    int rowId,
    int level,
    VectorizedValuesReader data) throws IOException {
    int left = total;
    c.precision = (short) type.asPrimitiveType().getDecimalMetadata().getPrecision();
    c.scale = (short) type.asPrimitiveType().getDecimalMetadata().getScale();
    while (left > 0) {
      consume();
      readRepetitionAndDefinitionLevels();
      if (definitionLevel >= level) {
        c.vector[rowId] = new HiveDecimalWritable(data.readBytes().getBytesUnsafe(),
          type.asPrimitiveType().getDecimalMetadata().getScale());
        c.isNull[rowId] = false;
        if(c.isRepeating){
          c.isRepeating = (c.vector[0] == c.vector[rowId]);
        }
      } else {
        c.isNull[rowId] = true;
        c.isRepeating = false;
        c.noNulls = false;
      }
      rowId++;
      left--;
    }
  }

  public void readBinarys(
    int total,
    BytesColumnVector c,
    int rowId,
    int level,
    VectorizedValuesReader data) throws IOException {
    int left = total;
    while (left > 0) {
      consume();
      readRepetitionAndDefinitionLevels();
      if (definitionLevel >= level) {
        Binary binary = data.readBytes();
        c.setVal(rowId, binary.getBytesUnsafe());
        c.isNull[rowId] = false;
        if(c.isRepeating){
          c.isRepeating = Arrays.equals(binary.getBytesUnsafe(),
            ArrayUtils.subarray(c.vector[0], c.start[0], c.length[0]));
        }
      } else {
        c.isNull[rowId] = true;
        c.isRepeating = false;
        c.noNulls = false;
      }
      rowId++;
      left--;
    }
  }

  /**
   * Reads `num` values into column, decoding the values from `dictionaryIds` and `dictionary`.
   */
  private void decodeDictionaryIds(int rowId, int num, ColumnVector column,
                                   LongColumnVector dictionaryIds) {
    System.arraycopy(dictionaryIds.isNull, rowId, column.isNull, rowId, num);
    if (column.noNulls) {
      column.noNulls = dictionaryIds.noNulls ? true : false;
    }
    if(column.isRepeating){
      column.isRepeating = dictionaryIds.isRepeating;
    }
    switch (descriptor.getType()) {
    case INT32:
      for (int i = rowId; i < rowId + num; ++i) {
        ((LongColumnVector) column).vector[i] =
          dictionary.decodeToInt((int) dictionaryIds.vector[i]);
      }
      break;
    case INT64:
      for (int i = rowId; i < rowId + num; ++i) {
        ((LongColumnVector) column).vector[i] =
          dictionary.decodeToLong((int) dictionaryIds.vector[i]);
      }
      break;
    case FLOAT:
      for (int i = rowId; i < rowId + num; ++i) {
        ((DoubleColumnVector) column).vector[i] =
          dictionary.decodeToFloat((int) dictionaryIds.vector[i]);
      }
      break;
    case DOUBLE:
      for (int i = rowId; i < rowId + num; ++i) {
        ((DoubleColumnVector) column).vector[i] =
          dictionary.decodeToDouble((int) dictionaryIds.vector[i]);
      }
      break;
    case INT96:
      for (int i = rowId; i < rowId + num; ++i) {
        ByteBuffer buf = dictionary.decodeToBinary((int) dictionaryIds.vector[i]).toByteBuffer();
        buf.order(ByteOrder.LITTLE_ENDIAN);
        long timeOfDayNanos = buf.getLong();
        int julianDay = buf.getInt();
        NanoTime nt = new NanoTime(julianDay, timeOfDayNanos);
        Timestamp ts = getTimestamp(nt, skipTimestampConversion);
        ((TimestampColumnVector) column).set(i, ts);
      }
      break;
    case BINARY:
    case FIXED_LEN_BYTE_ARRAY:
      for (int i = rowId; i < rowId + num; ++i) {
        ((BytesColumnVector) column)
          .setVal(i, dictionary.decodeToBinary((int) dictionaryIds.vector[i]).getBytesUnsafe());
      }
      break;
    default:
      throw new UnsupportedOperationException("Unsupported type: " + descriptor.getType());
    }
  }

  public static Timestamp getTimestamp(
    NanoTime nt,
    boolean skipConversion) {
    int julianDay = nt.getJulianDay();
    long nanosOfDay = nt.getTimeOfDayNanos();

    long remainder = nanosOfDay;
    julianDay += remainder / NANOS_PER_DAY;
    remainder %= NANOS_PER_DAY;
    if (remainder < 0) {
      remainder += NANOS_PER_DAY;
      julianDay--;
    }

    JDateTime jDateTime = new JDateTime((double) julianDay);
    Calendar calendar = getCalendar(skipConversion);
    calendar.set(Calendar.YEAR, jDateTime.getYear());
    calendar.set(Calendar.MONTH, jDateTime.getMonth() - 1); //java calendar index starting at 1.
    calendar.set(Calendar.DAY_OF_MONTH, jDateTime.getDay());

    int hour = (int) (remainder / (NANOS_PER_HOUR));
    remainder = remainder % (NANOS_PER_HOUR);
    int minutes = (int) (remainder / (NANOS_PER_MINUTE));
    remainder = remainder % (NANOS_PER_MINUTE);
    int seconds = (int) (remainder / (NANOS_PER_SECOND));
    long nanos = remainder % NANOS_PER_SECOND;

    calendar.set(Calendar.HOUR_OF_DAY, hour);
    calendar.set(Calendar.MINUTE, minutes);
    calendar.set(Calendar.SECOND, seconds);
    Timestamp ts = new Timestamp(calendar.getTimeInMillis());
    ts.setNanos((int) nanos);
    return ts;
  }

  private static Calendar getCalendar(boolean skipConversion) {
    Calendar calendar = skipConversion ? getLocalCalendar() : getGMTCalendar();
    calendar.clear(); // Reset all fields before reusing this instance
    return calendar;
  }

  private static Calendar getGMTCalendar() {
    //Calendar.getInstance calculates the current-time needlessly, so cache an instance.
    if (parquetGMTCalendar.get() == null) {
      parquetGMTCalendar.set(Calendar.getInstance(TimeZone.getTimeZone("GMT")));
    }
    return parquetGMTCalendar.get();
  }

  private static Calendar getLocalCalendar() {
    if (parquetLocalCalendar.get() == null) {
      parquetLocalCalendar.set(Calendar.getInstance());
    }
    return parquetLocalCalendar.get();
  }

  private void readPage() throws IOException {
    DataPage page = pageReader.readPage();
    // TODO: Why is this a visitor?
    page.accept(new DataPage.Visitor<Void>() {
      @Override
      public Void visit(DataPageV1 dataPageV1) {
        try {
          readPageV1(dataPageV1);
          return null;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public Void visit(DataPageV2 dataPageV2) {
        try {
          readPageV2(dataPageV2);
          return null;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    });
  }

  private void initDataReader(Encoding dataEncoding, byte[] bytes, int offset) throws IOException {
    this.endOfPageValueCount = valuesRead + pageValueCount;
    if (dataEncoding.usesDictionary()) {
      this.dataColumn = null;
      if (dictionary == null) {
        throw new IOException(
          "could not read page in col " + descriptor +
            " as the dictionary was missing for encoding " + dataEncoding);
      }
      @SuppressWarnings("deprecation")
      Encoding plainDict = Encoding.PLAIN_DICTIONARY; // var to allow warning suppression
      if (dataEncoding != plainDict && dataEncoding != Encoding.RLE_DICTIONARY) {
        throw new UnsupportedOperationException("Unsupported encoding: " + dataEncoding);
      }
      this.dataColumn = new VectorizedRleValuesReader();
      this.isCurrentPageDictionaryEncoded = true;
    } else {
      if (dataEncoding != Encoding.PLAIN) {
        throw new UnsupportedOperationException("Unsupported encoding: " + dataEncoding);
      }
      this.dataColumn = new VectorizedPlainValuesReader();
      this.isCurrentPageDictionaryEncoded = false;
    }

    try {
      dataColumn.initFromPage(pageValueCount, bytes, offset);
    } catch (IOException e) {
      throw new IOException("could not read page in col " + descriptor, e);
    }
  }

  private void readPageV1(DataPageV1 page) throws IOException {
    this.pageValueCount = page.getValueCount();
    ValuesReader rlReader = page.getRlEncoding().getValuesReader(descriptor, REPETITION_LEVEL);
    ValuesReader dlReader = page.getDlEncoding().getValuesReader(descriptor, DEFINITION_LEVEL);

    this.repetitionLevelColumn = new VectorizedParquetRecordReader.ValuesReaderIntIterator(rlReader);
    this.definitionLevelColumn = new VectorizedParquetRecordReader.ValuesReaderIntIterator(dlReader);
    // Initialize the decoders.
    if (page.getDlEncoding() != Encoding.RLE && descriptor.getMaxDefinitionLevel() != 0) {
      throw new UnsupportedOperationException("Unsupported encoding: " + page.getDlEncoding());
    }
    int bitWidth = BytesUtils.getWidthFromMaxInt(descriptor.getMaxDefinitionLevel());
    this.defColumn =
      new VectorizedRleValuesReader(bitWidth, definitionLevelColumn, repetitionLevelColumn);
    try {
      byte[] bytes = page.getBytes().toByteArray();
      rlReader.initFromPage(pageValueCount, bytes, 0);
      int next = rlReader.getNextOffset();
      dlReader.initFromPage(pageValueCount, bytes, next);
      next = dlReader.getNextOffset();
      initDataReader(page.getValueEncoding(), bytes, next);
    } catch (IOException e) {
      throw new IOException("could not read page " + page + " in col " + descriptor, e);
    }
  }

  private void readPageV2(DataPageV2 page) throws IOException {
    this.pageValueCount = page.getValueCount();
    this.repetitionLevelColumn = createRLEIterator(descriptor.getMaxRepetitionLevel(),
      page.getRepetitionLevels(), descriptor);
    this.definitionLevelColumn = newRLEIterator(descriptor.getMaxDefinitionLevel(), page
      .getDefinitionLevels());

    int bitWidth = BytesUtils.getWidthFromMaxInt(descriptor.getMaxDefinitionLevel());
    this.defColumn =
      new VectorizedRleValuesReader(bitWidth, definitionLevelColumn, repetitionLevelColumn);
    this.definitionLevelColumn = new VectorizedParquetRecordReader.ValuesReaderIntIterator(this
      .defColumn);
    this.defColumn.initFromBuffer(this.pageValueCount, page.getDefinitionLevels().toByteArray());
    try {
      initDataReader(page.getDataEncoding(), page.getData().toByteArray(), 0);
    } catch (IOException e) {
      throw new IOException("could not read page " + page + " in col " + descriptor, e);
    }
  }

  private void readRepetitionAndDefinitionLevels() {
    repetitionLevel = repetitionLevelColumn.nextInt();
    definitionLevel = definitionLevelColumn.nextInt();
    valuesRead++;
  }

  private VectorizedParquetRecordReader.IntIterator newRLEIterator(int maxLevel, BytesInput bytes) {
    try {
      if (maxLevel == 0) {
        return new VectorizedParquetRecordReader.NullIntIterator();
      }
      return new VectorizedParquetRecordReader.RLEIntIterator(
        new RunLengthBitPackingHybridDecoder(
          BytesUtils.getWidthFromMaxInt(maxLevel),
          new ByteArrayInputStream(bytes.toByteArray())));
    } catch (IOException e) {
      throw new ParquetDecodingException(
        "could not read levels in page for col " + descriptor, e);
    }
  }

  /**
   * Creates a reader for definition and repetition levels, returning an optimized one if
   * the levels are not needed.
   */
  protected static VectorizedParquetRecordReader.IntIterator createRLEIterator(int maxLevel,
                                                                               BytesInput bytes,
                                                 ColumnDescriptor descriptor) throws IOException {
    try {
      if (maxLevel == 0) return new VectorizedParquetRecordReader.NullIntIterator();
      return new VectorizedParquetRecordReader.RLEIntIterator(
        new RunLengthBitPackingHybridDecoder(
          BytesUtils.getWidthFromMaxInt(maxLevel),
          new ByteArrayInputStream(bytes.toByteArray())));
    } catch (IOException e) {
      throw new IOException("could not read levels in page for col " + descriptor, e);
    }
  }

}
