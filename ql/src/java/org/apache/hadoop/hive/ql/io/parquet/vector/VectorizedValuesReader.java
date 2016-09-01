package org.apache.hadoop.hive.ql.io.parquet.vector;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.parquet.io.api.Binary;

public interface VectorizedValuesReader {
  boolean readBoolean();
  byte readByte();
  int readInteger();
  long readLong();
  float readFloat();
  double readDouble();
  Binary readBytes();

  /*
   * Reads `total` values into `c` start at `c[rowId]`
   */
  void readBooleans(int total, LongColumnVector c, int rowId);
  void readBytes(int total, LongColumnVector c, int rowId);
  void readIntegers(int total, LongColumnVector c, int rowId);
  void readLongs(int total, LongColumnVector c, int rowId);
  void readFloats(int total, DoubleColumnVector c, int rowId);
  void readDoubles(int total, DoubleColumnVector c, int rowId);
  void readBinarys(int total, BytesColumnVector c, int rowId);
}
