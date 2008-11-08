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

import java.util.Iterator;
import java.util.Map;

/**
 * @author Jeremy Chow(coderplay@gmail.com)
 */
public abstract class AbstractVector implements Vector {

  public class Element implements Vector.Element {
    protected int index;

    public Element(int index) {
      this.index = index;
    }

    public double getValue() {
      return get(index);
    }

    public int getIndex() {
      return index;
    }

    public void setValue(double value) {
      set(index, value);
    }
  }
  
  public double get(int index) throws IndexException {
    if (index >= 0 && index < cardinality())
      return getQuick(index);
    else
      throw new IndexException();
  }
  
  public void set(int index, double value) throws IndexException{
    if (index >= 0 && index < cardinality())
      setQuick(index, value);
    else
      throw new IndexException();
  }
  
  public Vector add(double value) {
    Vector result = copy();
    for (Vector.Element element : this) {
      result.setQuick(element.getIndex(), getQuick(element.getIndex()) + value);
    }
    return result;
  }

  public Vector add(Vector v) throws CardinalityException{
    if (cardinality() != v.cardinality())
      throw new CardinalityException();
    Vector result = copy();
    for (Vector.Element element : v) {
      result.setQuick(element.getIndex(), getQuick(element.getIndex())
          + element.getValue());
    }
    return result;
  }

  public double dot(Vector v) throws CardinalityException{
    if (cardinality() != v.cardinality())
      throw new CardinalityException();
    double result = 0;
    for (Vector.Element element : v) {
      result += getQuick(element.getIndex()) * element.getValue();
    }
    return result;
  }

  public Vector scale(double alpha) {
    Vector result = copy();
    for (Vector.Element element : this) {
      result.setQuick(element.getIndex(), getQuick(element.getIndex()) * alpha);
    }
    return result;
  }

  public abstract int cardinality();

  public abstract Vector copy();

  public abstract double getQuick(int index);

  public abstract void setQuick(int index, double value);

  public abstract int size();

  public abstract Iterator<Vector.Element> iterator();

  public abstract Vector like();

  public abstract Vector like(int cardinality);

  public abstract String asFormatString();
}
