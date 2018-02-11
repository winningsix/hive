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

import org.apache.parquet.column.Dictionary;

import java.io.IOException;
import java.sql.Timestamp;

public interface ParquetDataColumnReader {
  void initFromPage(int valueCount, byte[] page, int offset) throws IOException;

  int readValueDictionaryId();

  long readLong();

  int readInteger();

  float readFloat();

  boolean readBoolean();

  byte[] readString();
  
  byte[] readVarchar();
  
  byte[] readChar();

  byte[] readBytes();

  byte[] readDecimal();

  double readDouble();

  Timestamp readTimestamp();

  Dictionary getDictionary();

  byte[] readBytes(int id);

  float readFloat(int id);

  double readDouble(int id);

  int readInteger(int id);

  long readLong(int id);

  boolean readBoolean(int id);

  byte[] readDecimal(int id);

  Timestamp readTimestamp(int id);

  byte[] readString(int id);
  
  byte[] readVarchar(int id);
  
  byte[] readChar(int id);
}
