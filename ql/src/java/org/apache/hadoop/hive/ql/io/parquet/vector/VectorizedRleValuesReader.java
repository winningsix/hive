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

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.parquet.Preconditions;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.bitpacking.BytePacker;
import org.apache.parquet.column.values.bitpacking.Packer;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.api.Binary;

import java.io.IOException;
import java.util.Arrays;

public class VectorizedRleValuesReader extends ValuesReader implements VectorizedValuesReader {


  // Current decoding mode. The encoded data contains groups of either run length encoded data
  // (RLE) or bit packed data. Each group contains a header that indicates which group it is and
  // the number of values in the group.
  // More details here: https://github.com/Parquet/parquet-format/blob/master/Encodings.md
  private enum MODE {
    RLE,
    PACKED
  }

  // Encoded data.
  private byte[] in;
  private int end;
  private int offset;

  // bit/byte width of decoded data and utility to batch unpack them.
  private int bitWidth;
  private int bytesWidth;
  private BytePacker packer;

  // Current decoding mode and values
  private MODE mode;
  private int currentCount;
  private int currentValue;

  private int definitionLevel;

  private VectorizedParquetRecordReader.IntIterator repetitionLevelColumn;
  private VectorizedParquetRecordReader.IntIterator definitionLevelColumn;

  // Buffer of decoded values if the values are PACKED.
  private int[] currentBuffer = new int[16];
  private int currentBufferIdx = 0;

  // If true, the bit width is fixed. This decoder is used in different places and this also
  // controls if we need to read the bitwidth from the beginning of the data stream.
  private final boolean fixedWidth;

  public VectorizedRleValuesReader() {
    fixedWidth = false;
  }

  public VectorizedRleValuesReader(
    int bitWidth,
    VectorizedParquetRecordReader.IntIterator definitionLevelColumn,
    VectorizedParquetRecordReader.IntIterator repetitionLevelColumn) {
    fixedWidth = true;
    this.definitionLevelColumn = definitionLevelColumn;
    this.repetitionLevelColumn = repetitionLevelColumn;
//    readRepetitionAndDefinitionLevels();
    init(bitWidth);
  }

  private void readRepetitionAndDefinitionLevels() {
//    repetitionLevel = repetitionLevelColumn.nextInt();
    definitionLevel = definitionLevelColumn.nextInt();
  }

  @Override
  public int getNextOffset() {
    return this.end;
  }

  @Override
  public void initFromPage(
    int valueCount,
    byte[] page,
    int start) throws IOException {
    this.offset = start;
    this.in = page;
    if (fixedWidth) {
      if (bitWidth != 0) {
        int length = readIntLittleEndian();
        this.end = this.offset + length;
      }
    } else {
      this.end = page.length;
      if (this.end != this.offset) init(page[this.offset++] & 255);
    }
    if (bitWidth == 0) {
      // 0 bit width, treat this as an RLE run of valueCount number of 0's.
      this.mode = MODE.RLE;
      this.currentCount = valueCount;
      this.currentValue = 0;
    } else {
      this.currentCount = 0;
    }
  }

  /**
   * Initializes the internal state for decoding ints of `bitWidth`.
   */
  private void init(int bitWidth) {
    Preconditions.checkArgument(bitWidth >= 0 && bitWidth <= 32, "bitWidth must be >= 0 and <= 32");
    this.bitWidth = bitWidth;
    this.bytesWidth = BytesUtils.paddedByteCountFromBits(bitWidth);
    this.packer = Packer.LITTLE_ENDIAN.newBytePacker(bitWidth);
  }

  @Override
  public void skip() {
    this.readInteger();
  }

  @Override
  public byte readByte() {
    throw new UnsupportedOperationException("only readInts is valid.");
  }

  @Override
  public int readInteger() {
    if (this.currentCount == 0) { this.readNextGroup(); }

    this.currentCount--;
    switch (mode) {
    case RLE:
      return this.currentValue;
    case PACKED:
      return this.currentBuffer[currentBufferIdx++];
    }
    throw new RuntimeException("Unreachable");
  }

  @Override
  public Binary readBytes() {
    throw new UnsupportedOperationException("only readInts is valid.");
  }

  @Override
  public void readBooleans(
    int total,
    LongColumnVector c,
    int rowId) {
    throw new UnsupportedOperationException("only readInts is valid.");
  }

  @Override
  public void readBytes(
    int total,
    LongColumnVector c,
    int rowId) {
    throw new UnsupportedOperationException("only readInts is valid.");
  }

  // The RLE reader implements the vectorized decoding interface when used to decode dictionary
  // IDs. This is different than the above APIs that decodes definitions levels along with values.
  // Since this is only used to decode dictionary IDs, only decoding integers is supported.
  @Override
  public void readLongs(
    int total,
    LongColumnVector c,
    int rowId) {
    throw new UnsupportedOperationException("only readInts is valid.");
  }

  @Override
  public void readFloats(
    int total,
    DoubleColumnVector c,
    int rowId) {
    throw new UnsupportedOperationException("only readInts is valid.");
  }

  @Override
  public void readDoubles(
    int total,
    DoubleColumnVector c,
    int rowId) {
    throw new UnsupportedOperationException("only readInts is valid.");
  }

  @Override
  public void readBinarys(
    int total,
    BytesColumnVector c,
    int rowId) {
    throw new UnsupportedOperationException("only readInts is valid.");
  }

  @Override
  public void readIntegers(
    int total,
    LongColumnVector c,
    int rowId) {
    int left = total;
    while (left > 0) {
      if (this.currentCount == 0)
        this.readNextGroup();
      int n = Math.min(left, this.currentCount);
      switch (mode) {
      case RLE:
        Arrays.fill(c.vector, rowId, rowId + n, currentValue);
        if (c.isRepeating) {
          c.isRepeating = (c.vector[0] == currentValue);
        }
        break;
      case PACKED:
        for (int i = 0; i < n; i++) {
          c.vector[rowId + i] = currentBuffer[currentBufferIdx + i];
        }
        int i = 0;
        while (c.isRepeating && i < n) {
          c.isRepeating = (c.vector[0] == currentBuffer[i]);
          i++;
        }
        currentBufferIdx += n;
        break;
      }
      rowId += n;
      left -= n;
      currentCount -= n;
    }
  }

  // Initialize the reader from a buffer. This is used for the V2 page encoding where the
  // definition are in its own buffer.
  public void initFromBuffer(int valueCount, byte[] data) {
    this.offset = 0;
    this.in = data;
    this.end = data.length;
    if (bitWidth == 0) {
      // 0 bit width, treat this as an RLE run of valueCount number of 0's.
      this.mode = MODE.RLE;
      this.currentCount = valueCount;
      this.currentValue = 0;
    } else {
      this.currentCount = 0;
    }
  }

  /**
   * Reads the next varint encoded int.
   */
  private int readUnsignedVarInt() {
    int value = 0;
    int shift = 0;
    int b;
    do {
      b = in[offset++] & 255;
      value |= (b & 0x7F) << shift;
      shift += 7;
    } while ((b & 0x80) != 0);
    return value;
  }

  /**
   * Reads the next 4 byte little endian int.
   */
  private int readIntLittleEndian() {
    int ch4 = in[offset] & 255;
    int ch3 = in[offset + 1] & 255;
    int ch2 = in[offset + 2] & 255;
    int ch1 = in[offset + 3] & 255;
    offset += 4;
    return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
  }

  /**
   * Reads the next byteWidth little endian int.
   */
  private int readIntLittleEndianPaddedOnBitWidth() {
    switch (bytesWidth) {
    case 0:
      return 0;
    case 1:
      return in[offset++] & 255;
    case 2: {
      int ch2 = in[offset] & 255;
      int ch1 = in[offset + 1] & 255;
      offset += 2;
      return (ch1 << 8) + ch2;
    }
    case 3: {
      int ch3 = in[offset] & 255;
      int ch2 = in[offset + 1] & 255;
      int ch1 = in[offset + 2] & 255;
      offset += 3;
      return (ch1 << 16) + (ch2 << 8) + (ch3 << 0);
    }
    case 4: {
      return readIntLittleEndian();
    }
    }
    throw new RuntimeException("Unreachable");
  }

  private int ceil8(int value) {
    return (value + 7) / 8;
  }

  /**
   * Reads the next group.
   */
  private void readNextGroup()  {
    int header = readUnsignedVarInt();
    this.mode = (header & 1) == 0 ? MODE.RLE : MODE.PACKED;
    switch (mode) {
    case RLE:
      this.currentCount = header >>> 1;
      this.currentValue = readIntLittleEndianPaddedOnBitWidth();
      return;
    case PACKED:
      int numGroups = header >>> 1;
      this.currentCount = numGroups * 8;
      int bytesToRead = ceil8(this.currentCount * this.bitWidth);

      if (this.currentBuffer.length < this.currentCount) {
        this.currentBuffer = new int[this.currentCount];
      }
      currentBufferIdx = 0;
      int valueIndex = 0;
      for (int byteIndex = offset; valueIndex < this.currentCount; byteIndex += this.bitWidth) {
        this.packer.unpack8Values(in, byteIndex, this.currentBuffer, valueIndex);
        valueIndex += 8;
      }
      offset += bytesToRead;
      return;
    default:
      throw new ParquetDecodingException("not a valid mode " + this.mode);
    }
  }
}
