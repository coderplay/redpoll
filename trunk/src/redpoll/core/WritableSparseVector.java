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

package redpoll.core;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Implements vector that only stores non-zero doubles and with {@link org.apache.hadoop.io.Writable} features.
 * @author Jeremy Chow(coderplay@gmail.com)
 */
public class WritableSparseVector extends SparseVector implements WritableVector {

  public WritableSparseVector() {
    super();
  }
  
  public WritableSparseVector(int cardinality) {
    super(cardinality);
  }
  
  public void readFields(DataInput dataInput) throws IOException {
    int cardinality = dataInput.readInt();
    int size = dataInput.readInt();
    Map<Integer, Double> values = new HashMap<Integer, Double>(size);
    for (int i = 0; i < size; i++) {
      values.put(dataInput.readInt(), dataInput.readDouble());
    }
    this.cardinality = cardinality;
    this.values = values;
  }
  
  public void write(DataOutput out) throws IOException {
    out.writeInt(cardinality());
    out.writeInt(size());
    for (Vector.Element element : this) {
      if (element.getValue() != 0d) {
        out.writeInt(element.getIndex());
        out.writeDouble(element.getValue());
      }
    }
  }
  
  
  @Override
  public WritableSparseVector like() {
    return new WritableSparseVector(cardinality);
  }
  
  @Override
  public WritableSparseVector like(int newCardinality) {
    return new WritableSparseVector(newCardinality);
  }
  
  /**
   * It's useful when outputing in {@link TextOutputFormat}.
   */
  public String toString() {
    return asFormatString();
  }
  
}
