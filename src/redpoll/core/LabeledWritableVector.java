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
import java.util.Iterator;

import org.apache.hadoop.io.Text;

/**
 * Wrapper of a vector with a name.
 * @author Jeremy Chow(coderplay@gmail.com)
 */
public class LabeledWritableVector implements WritableVector {
  
  private Text label;
  private WritableVector vector;
  
  public LabeledWritableVector() {
    label = new Text();
    vector = new WritableSparseVector(0);
  }
    
  protected LabeledWritableVector(int cardinality) {
    label = new Text();
    vector = new WritableSparseVector(cardinality);
  }
  
  
  public LabeledWritableVector(Text label, WritableVector vector) {
    this.label = label;
    this.vector = vector;
  }
  
  public Vector add(double value) {
    return vector.add(value);
  }

  public Vector add(Vector v) {
    return vector.add(v);
  }

  public String asFormatString() {
    return label.toString() + ":" + vector.asFormatString();
  }

  public int cardinality() {
    return vector.cardinality();
  }

  public LabeledWritableVector copy() {
    return new LabeledWritableVector(new Text(label), vector.copy()); 
  }

  public double dot(Vector v) {
    return vector.dot(v);
  }

  public double get(int index) {
    return vector.get(index);
  }

  public double getQuick(int index) {
    return vector.getQuick(index);
  }

  public Iterator<Element> iterator() {
    return vector.iterator();
  }

  public LabeledWritableVector like() {
    return new LabeledWritableVector(vector.cardinality());
  }

  public LabeledWritableVector like(int cardinality) {
    return new LabeledWritableVector(cardinality);
  }

  public Vector scale(double alpha) {
    return vector.scale(alpha);
  }

  public void set(int index, double value) {
    vector.set(index, value);
  }

  public void setQuick(int index, double value) {
    vector.setQuick(index, value);
  }

  public int size() {
    return vector.size();
  }

  public void readFields(DataInput in) throws IOException {
    label.readFields(in);
    vector.readFields(in);
  }

  public void write(DataOutput out) throws IOException {
    label.write(out);
    vector.write(out);
  }
  
  public Text getLabel() {
    return label;
  }
  
  public WritableVector getVector() {
    return vector;
  }
  
  public String toString() {
    return asFormatString();
  }

}
