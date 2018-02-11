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

package org.apache.hadoop.hive.ql.io.parquet.vector;

import org.apache.hadoop.hive.ql.io.parquet.timestamp.NanoTime;
import org.apache.hadoop.hive.ql.io.parquet.timestamp.NanoTimeUtils;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Timestamp;

/**
 * Parquet file has self-describing schema which may differ from the user required schema (e.g.
 * schema evolution). This factory is used to retrieve user required typed data via corresponding
 * reader which reads the underlying data.
 */
public final class ParquetDataColumnReaderFactory {

  private ParquetDataColumnReaderFactory() {
  }

  /**
   * The default data column reader for existing Parquet page reader which works for both
   * dictionary or non dictionary types, Mirror from dictionary encoding path.
   */
  public static class DefaultParquetDataColumnReader implements ParquetDataColumnReader {
    protected ValuesReader valuesReader;
    protected Dictionary dict;

    public DefaultParquetDataColumnReader(ValuesReader valuesReader) {
      this.valuesReader = valuesReader;
    }

    public DefaultParquetDataColumnReader(Dictionary dict) {
      this.dict = dict;
    }

    public void initFromPage(int i, ByteBuffer byteBuffer, int i1) throws IOException {
      valuesReader.initFromPage(i, byteBuffer, i1);
    }

    public void initFromPage(int valueCount, byte[] page, int offset) throws IOException {
      this.initFromPage(valueCount, ByteBuffer.wrap(page), offset);
    }

    /**
     * @return the next boolean from the page
     */
    public boolean readBoolean() {
      return valuesReader.readBoolean();
    }

    public boolean readBoolean(int id){
      return dict.decodeToBoolean(id);
    }


    @Override
    public byte[] readString(int id) {
      return dict.decodeToBinary(id).getBytesUnsafe();
    }

    @Override
    public byte[] readString() {
      return valuesReader.readBytes().getBytesUnsafe();
    }

    /**
     * @return the next Binary from the page
     */
    public byte[] readBytes() {
      return valuesReader.readBytes().getBytesUnsafe();
    }

    public byte[] readBytes(int id) {
      return dict.decodeToBinary(id).getBytesUnsafe();
    }

    @Override
    public byte[] readDecimal() {
      return valuesReader.readBytes().getBytesUnsafe();
    }

    @Override
    public byte[] readDecimal(int id) {
      return dict.decodeToBinary(id).getBytesUnsafe();
    }

    /**
     * @return the next float from the page
     */
    public float readFloat() {
      return valuesReader.readFloat();
    }

    public float readFloat(int id) {
      return dict.decodeToFloat(id);
    }

    /**
     * @return the next double from the page
     */
    public double readDouble() {
      return valuesReader.readDouble();
    }

    public double readDouble(int id) {
      return dict.decodeToDouble(id);
    }

    @Override
    public Timestamp readTimestamp() {
      throw new RuntimeException("Unsupported operation");
    }

    @Override
    public Timestamp readTimestamp(int id) {
      throw new RuntimeException("Unsupported operation");
    }

    /**
     * @return the next integer from the page
     */
    public int readInteger() {
      return valuesReader.readInteger();
    }

    public int readInteger(int id) {
      return dict.decodeToInt(id);
    }

    public long readLong(int id) {
      return dict.decodeToLong(id);
    }

    /**
     * @return the next long from the page
     */
    public long readLong() {
      return valuesReader.readLong();
    }

    public int readValueDictionaryId() {
      return valuesReader.readValueDictionaryId();
    }

    public void skip() {
      valuesReader.skip();
    }

    @Override
    public Dictionary getDictionary() {
      return dict;
    }
  }

  /**
   * The reader who reads from the underlying int32 value value. Implementation is in consist with
   * ETypeConverter EINT32_CONVERTER
   */
  public static class TypesFromInt32PageReader extends DefaultParquetDataColumnReader {

    public TypesFromInt32PageReader(ValuesReader realReader) {
      super(realReader);
    }

    public TypesFromInt32PageReader(Dictionary dict) {
      super(dict);
    }

    @Override
    public long readLong() {
      return valuesReader.readInteger();
    }

    @Override
    public long readLong(int id) {
      return dict.decodeToInt(id);
    }

    @Override
    public float readFloat() {
      return valuesReader.readInteger();
    }

    @Override
    public float readFloat(int id) {
      return dict.decodeToInt(id);
    }

    @Override
    public double readDouble() {
      return valuesReader.readInteger();
    }

    public double readDouble(int id) {
      return dict.decodeToInt(id);
    }

    private byte[] convertToString(int value) {
      try {
        // convert integer to string
        return Integer.toString(value).getBytes("UTF-8");
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException("Failed to encode string in UTF-8", e);
      }
    }

    @Override
    public byte[] readString() {
      return convertToString(valuesReader.readInteger());
    }

    @Override
    public byte[] readString(int id) {
      return convertToString(dict.decodeToInt(id));
    }
  }

  /**
   * The reader who reads from the underlying int64 value value. Implementation is in consist with
   * ETypeConverter EINT64_CONVERTER
   */
  public static class TypesFromInt64PageReader extends DefaultParquetDataColumnReader {

    public TypesFromInt64PageReader(ValuesReader realReader) {
      super(realReader);
    }

    public TypesFromInt64PageReader(Dictionary dictionary) {
      super(dictionary);
    }

    @Override
    public float readFloat() {
      return valuesReader.readLong();
    }

    @Override
    public float readFloat(int id) {
      return dict.decodeToLong(id);
    }

    @Override
    public double readDouble() {
      return valuesReader.readLong();
    }

    public double readDouble(int id) {
      return dict.decodeToLong(id);
    }

    private byte[] convertToByte(long value) {
      try {
        // convert long to string
        return Long.toString(value).getBytes("UTF-8");
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException("Failed to encode string in UTF-8", e);
      }
    }

    @Override
    public byte[] readString(int id) {
      return convertToByte(dict.decodeToLong(id));
    }

    @Override
    public byte[] readString() {
      return convertToByte(valuesReader.readLong());
    }
  }

  /**
   * The reader who reads from the underlying float value value. Implementation is in consist with
   * ETypeConverter EFLOAT_CONVERTER
   */
  public static class TypesFromFloatPageReader extends DefaultParquetDataColumnReader {

    public TypesFromFloatPageReader(ValuesReader realReader) {
      super(realReader);
    }

    public TypesFromFloatPageReader(Dictionary realReader) {
      super(realReader);
    }

    @Override
    public double readDouble() {
      return valuesReader.readFloat();
    }

    @Override
    public double readDouble(int id) {
      return dict.decodeToFloat(id);
    }

    private byte[] convert(float value) {
      try {
        // convert float to string
        return Float.toString(value).getBytes("UTF-8");
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException("Failed to encode string in UTF-8", e);
      }
    }

    @Override
    public byte[] readString() {
      return convert(valuesReader.readFloat());
    }

    @Override
    public byte[] readString(int id) {
      return convert(dict.decodeToFloat(id));
    }
  }

  /**
   * The reader who reads from the underlying double value value.
   */
  public static class TypesFromDoublePageReader extends DefaultParquetDataColumnReader {

    public TypesFromDoublePageReader(ValuesReader realReader) {
      super(realReader);
    }

    public TypesFromDoublePageReader(Dictionary realReader) {
      super(realReader);
    }

    private byte[] convert(double value) {
      try {
        // convert float to string
        return Double.toString(value).getBytes("UTF-8");
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException("Failed to encode string in UTF-8", e);
      }
    }

    @Override
    public byte[] readString() {
      return convert(valuesReader.readDouble());
    }

    @Override
    public byte[] readString(int id) {
      return convert(dict.decodeToDouble(id));
    }
  }

  /**
   * The reader who reads from the underlying boolean value value.
   */
  public static class TypesFromBooleanPageReader extends DefaultParquetDataColumnReader {

    public TypesFromBooleanPageReader(ValuesReader valuesReader) {
      super(valuesReader);
    }

    public TypesFromBooleanPageReader(Dictionary dict) {
      super(dict);
    }

    @Override
    public byte[] readString(int id) {
      final boolean value = dict.decodeToBoolean(id);

      try {
        // convert boolean to string
        return Boolean.toString(value).getBytes("UTF-8");
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException("Failed to encode string in UTF-8", e);
      }
    }

    @Override
    public byte[] readString() {
      final boolean value = valuesReader.readBoolean();

      try {
        // convert boolean to string
        return Boolean.toString(value).getBytes("UTF-8");
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException("Failed to encode string in UTF-8", e);
      }
    }
  }

  /**
   * The reader who reads from the underlying Timestamp value value.
   */
  public static class TypesFromInt96PageReader extends DefaultParquetDataColumnReader {
    private boolean skipTimestampConversion = false;

    public TypesFromInt96PageReader(ValuesReader realReader, boolean skipTimestampConversion) {
      super(realReader);
      this.skipTimestampConversion = skipTimestampConversion;
    }

    public TypesFromInt96PageReader(Dictionary dict, boolean skipTimestampConversion) {
      super(dict);
      this.skipTimestampConversion = skipTimestampConversion;
    }

    private Timestamp convert(Binary binary){
      ByteBuffer buf = binary.toByteBuffer();
      buf.order(ByteOrder.LITTLE_ENDIAN);
      long timeOfDayNanos = buf.getLong();
      int julianDay = buf.getInt();
      NanoTime nt = new NanoTime(julianDay, timeOfDayNanos);
      return NanoTimeUtils.getTimestamp(nt, skipTimestampConversion);
    }

    @Override
    public Timestamp readTimestamp(int id) {
      return convert(dict.decodeToBinary(id));
    }

    @Override
    public Timestamp readTimestamp() {
      return convert(valuesReader.readBytes());
    }

    @Override
    public byte[] readString() {
      //TODO use timestampwritable to do the convert?
      try {
        return readTimestamp().toString().getBytes("UTF-8");
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException("Failed to encode string in UTF-8", e);
      }
    }
    @Override
    public byte[] readString(int id) {
      //TODO use timestampwritable to do the convert?
      try {
        return readTimestamp(id).toString().getBytes("UTF-8");
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException("Failed to encode string in UTF-8", e);
      }
    }
  }

  public static class TypesFromDecimalPageReader extends DefaultParquetDataColumnReader {
    private HiveDecimalWritable tempDecimal = new HiveDecimalWritable();
    private short scale;

    public TypesFromDecimalPageReader(ValuesReader realReader, short scale) {
      super(realReader);
      this.scale = scale;
    }

    public TypesFromDecimalPageReader(Dictionary dict, short scale) {
      super(dict);
      this.scale = scale;
    }

    private byte[] convertToString(Binary value){
      tempDecimal.set(value.getBytesUnsafe(), scale);

      try {
        // convert boolean to string
        return tempDecimal.toString().getBytes("UTF-8");
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException("Failed to encode string in UTF-8", e);
      }
    }

    @Override
    public byte[] readString(int id) {
      return convertToString(dict.decodeToBinary(id));
    }

    @Override
    public byte[] readString() {
      return convertToString(valuesReader.readBytes());
    }
  }

  private static ParquetDataColumnReader getDataColumnReaderByTypeHelper(boolean isDictionary,
                                                                         PrimitiveType parquetType,
                                                                         Dictionary dictionary,
                                                                         ValuesReader valuesReader,
                                                                         boolean
                                                                             skipTimestampConversion)
      throws IOException {
    switch (parquetType.getPrimitiveTypeName()) {
    case INT32:
      return isDictionary ? new TypesFromInt32PageReader(dictionary) : new
          TypesFromInt32PageReader(valuesReader);
    case INT64:
      return isDictionary ? new TypesFromInt64PageReader(dictionary) : new
          TypesFromInt64PageReader(valuesReader);
    case FLOAT:
      return isDictionary ? new TypesFromFloatPageReader(dictionary) : new
          TypesFromFloatPageReader(valuesReader);
    case INT96:
      return isDictionary ? new TypesFromInt96PageReader(dictionary, skipTimestampConversion) : new
          TypesFromInt96PageReader(valuesReader, skipTimestampConversion);
    case BOOLEAN:
      return isDictionary ? new TypesFromBooleanPageReader(dictionary) : new
          TypesFromBooleanPageReader(valuesReader);
    case BINARY:
    case FIXED_LEN_BYTE_ARRAY:
      return getConvertorFromBinary(isDictionary, parquetType, valuesReader, dictionary);
    case DOUBLE:
      return isDictionary ? new TypesFromDoublePageReader(dictionary) : new
          TypesFromDoublePageReader(valuesReader);
    default:
      return isDictionary ? new DefaultParquetDataColumnReader(dictionary) : new
          DefaultParquetDataColumnReader(valuesReader);
    }
  }

  private static ParquetDataColumnReader getConvertorFromBinary(boolean isDict,
                                                                PrimitiveType parquetType,
                                                                ValuesReader valuesReader,
                                                                Dictionary dictionary) {
    OriginalType originalType = parquetType.getOriginalType();

    if (originalType == null) {
      return isDict ? new DefaultParquetDataColumnReader(dictionary) : new
          DefaultParquetDataColumnReader(valuesReader);
    }
    switch (originalType) {
    case DECIMAL:
      final short scale = (short) parquetType.asPrimitiveType().getDecimalMetadata().getScale();
      return isDict ? new TypesFromDecimalPageReader(dictionary, scale) : new
          TypesFromDecimalPageReader(valuesReader, scale);
    default:
      return isDict ? new DefaultParquetDataColumnReader(dictionary) : new
          DefaultParquetDataColumnReader(valuesReader);
    }
  }

  public static ParquetDataColumnReader getDataColumnReaderByTypeOnDictionary(
      PrimitiveType parquetType,
      Dictionary realReader, boolean skipTimestampConversion)
      throws IOException {
    return getDataColumnReaderByTypeHelper(true, parquetType, realReader, null,
        skipTimestampConversion);
  }

  public static ParquetDataColumnReader getDataColumnReaderByType(PrimitiveType parquetType,
                                                                  ValuesReader realReader,
                                                                  boolean skipTimestampConversion)
      throws IOException {
    return getDataColumnReaderByTypeHelper(false, parquetType, null, realReader,
        skipTimestampConversion);
  }
}
