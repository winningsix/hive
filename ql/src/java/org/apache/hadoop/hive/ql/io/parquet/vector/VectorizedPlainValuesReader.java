package org.apache.hadoop.hive.ql.io.parquet.vector;

import jodd.util.ArraysUtil;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.parquet.bytes.*;
import org.apache.parquet.bytes.LittleEndianDataInputStream;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.bitpacking.ByteBitPackingValuesReader;
import org.apache.parquet.io.api.Binary;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.apache.parquet.column.values.bitpacking.Packer.LITTLE_ENDIAN;

public class VectorizedPlainValuesReader extends ValuesReader implements VectorizedValuesReader{
  protected org.apache.parquet.bytes.LittleEndianDataInputStream in;
  private ByteBitPackingValuesReader booleanStream;
  private byte[] buffer;
  private int offset;

  @Override
  public void initFromPage(
    int valueCount,
    byte[] bytes,
    int offset) throws IOException {
    this.buffer = bytes;
    this.in = new LittleEndianDataInputStream(
      new ByteArrayInputStream(bytes, offset, bytes.length - offset));
    booleanStream = new ByteBitPackingValuesReader(1,
      LITTLE_ENDIAN);
    this.offset = offset;
    booleanStream.initFromPage(valueCount, bytes, offset);
  }

  @Override
  public void skip() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean readBoolean() {
    return booleanStream.readInteger() == 0 ? false : true;
  }

  @Override
  public int readInteger(){
    try {
      return in.readInt();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public long readLong(){
    try {
      return in.readLong();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public float readFloat(){
    try {
      return in.readFloat();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public double readDouble(){
    try {
      return in.readDouble();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void readLongs(
    int total,
    LongColumnVector c,
    int rowId) {
    try {
      for (int i = 0; i < total; i++) {
        c.vector[rowId + i] = in.readLong();
        if (c.isRepeating) {
          c.isRepeating = (c.vector[0] == c.vector[rowId + i]);
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void readFloats(
    int total,
    DoubleColumnVector c,
    int rowId) {
    try {
      for (int i = 0; i < total; i++) {
        c.vector[rowId + i] = in.readFloat();
        if (c.isRepeating) {
          c.isRepeating = (c.vector[0] == c.vector[rowId + i]);
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void readDoubles(
    int total,
    DoubleColumnVector c,
    int rowId) {
    try {
      for (int i = 0; i < total; i++) {
        c.vector[rowId + i] = in.readDouble();
        if (c.isRepeating) {
          c.isRepeating = (c.vector[0] == c.vector[rowId + i]);
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void readBinarys(
    int total,
    BytesColumnVector c,
    int rowId) {
    for (int i = 0; i < total; i++) {
      try {
        byte[] data = readBytes().getBytesUnsafe();
        c.setVal(rowId + i, data);
      } catch (Throwable e) {
        throw new RuntimeException("total " + total + " i " + i, e);
      }
      if (c.isRepeating) {
        c.isRepeating = Arrays.equals(ArraysUtil.subarray(c.vector[0], c.start[0], c.length[0]),
          ArraysUtil.subarray(c.vector[rowId + i], c.start[0], c.length[0]));
      }
    }
  }

  @Override
  public byte readByte() {
    try {
      return in.readByte();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Binary readBytes() {
    int length = 0;
    try {
      length = BytesUtils.readIntLittleEndian(buffer, offset);
    } catch (Throwable e) {
      throw new RuntimeException("Failed at " + length + " Buffer size " + buffer.length + " "
        + "offset " + offset, e);
    }
    int start = offset + 4;
    offset = start + length;
    return Binary.fromConstantByteArray(buffer, start, length);
  }

  @Override
  public void readBooleans(
    int total,
    LongColumnVector c,
    int rowId) {
    for (int i = 0; i < total; i++) {
      c.vector[rowId + i] = readBoolean() ? 1 : 0;
      if (c.isRepeating) {
        c.isRepeating = (c.vector[0] == c.vector[rowId + i]);
      }
    }
  }

  @Override
  public void readBytes(
    int total,
    LongColumnVector c,
    int rowId) {
    try {
      for (int i = 0; i < total; i++) {
        byte value = in.readByte();
        if (c.isRepeating) {
          c.isRepeating = (c.vector[0] == value);
        }
        c.vector[rowId + i] = value;
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void readIntegers(
    int total,
    LongColumnVector c,
    int rowId) {
    try {
      for (int i = 0; i < total; i++) {
        int value = in.readInt();
        if (c.isRepeating) {
          c.isRepeating = (c.vector[0] == value);
        }
        c.vector[rowId + i] = value;
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
