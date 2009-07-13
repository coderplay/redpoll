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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author Jeremy Chow(coderplay@gmail.com)
 */
public class SparseVector extends AbstractVector {
  
  protected Map<Integer, Double> values;
  
  protected int cardinality;
  
  
  public SparseVector() {}
  
  public SparseVector(int cardinality) {
    super();
    values = new HashMap<Integer, Double>();
    this.cardinality = cardinality;
  }
  
  @Override
  public int cardinality() {
    return cardinality;
  }

  @Override
  public SparseVector copy() {
    SparseVector result = like();
    for (Map.Entry<Integer, Double> entry : values.entrySet()) {
      result.setQuick(entry.getKey(), entry.getValue());
    }
    return result;
  }


  @Override
  public double getQuick(int index) {
    Double value = values.get(index);
    if (value == null)
      return 0.0;
    else
      return value;
  }

  @Override
  public Iterator<Vector.Element> iterator() {
    return new ElementIterator();
  }


  @Override
  public void setQuick(int index, double value) {
    if (value == 0.0)
      values.remove(index);
    else
      values.put(index, value);
  }

  @Override
  public int size() {
    return values.size();
  }

  @Override
  public SparseVector like() {
    return new SparseVector(cardinality);
  }
  
  @Override
  public String asFormatString() {
    StringBuilder out = new StringBuilder();
    out.append("[s").append(cardinality).append(", ");
    for (Map.Entry<Integer, Double> entry : values.entrySet()) {
      out.append(entry.getKey()).append(':').append(entry.getValue()).append(", ");
    }
    out.append("] ");
    return out.toString();
  }

  @Override
  public SparseVector like(int newCardinality) {
    return new SparseVector(newCardinality);
  }
    
  protected class ElementIterator implements java.util.Iterator<Vector.Element> {
    private final java.util.Iterator<Map.Entry<Integer, Double>> it;

    public ElementIterator() {
      it = values.entrySet().iterator();
    }

    public boolean hasNext() {
      return it.hasNext();
    }

    public Element next() {
      return new Element(it.next().getKey());
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
