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

import junit.framework.TestCase;

public class TestWritableSparseVector extends TestCase{
  
  final double[] values = { 1.1, 2.2, 3.3 };

  final Vector test = new WritableSparseVector(values.length + 2);
  
  public TestWritableSparseVector(String name) {
    super(name);
  }
  
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    for (int i = 0; i < values.length; i++)
      test.set(i + 1, values[i]);
  }

  public void testIterator() {
    int i = 0;
    for(Vector.Element element : test) {
      assertEquals(element.getValue(), values[i]);
      i++;
    }
  }
  
  public void testSize() throws Exception {
    assertEquals("size", 3, test.size());
  }

  public void testCopy() throws Exception {
    Vector copy = test.copy();
    for (int i = 0; i < test.cardinality(); i++)
      assertEquals("copy [" + i + "]", test.get(i), copy.get(i));
  }
  
  public void testGet() throws Exception {
    for (int i = 0; i < test.cardinality(); i++)
      if (i > 0 && i < 4)
        assertEquals("get [" + i + "]", values[i - 1], test.get(i));
      else
        assertEquals("get [" + i + "]", 0.0, test.get(i));
  }
  
  public void testCardinality() {
    assertEquals("cardinality", 5, test.cardinality());
  }
  
  public void testDot() throws Exception {
    double res = test.dot(test);
    assertEquals("dot", 1.1 * 1.1 + 2.2 * 2.2 + 3.3 * 3.3, res);
  }
  
  public void testScale() throws Exception {
    Vector val = test.scale(3);
    assertEquals("cardinality", test.cardinality(), val.cardinality());
    for (int i = 0; i < test.cardinality(); i++)
      if (i == 0 || i == 4)
        assertEquals("get [" + i + "]", 0.0, val.get(i));
      else
        assertEquals("get [" + i + "]", values[i - 1] * 3, val.get(i));
  }
  
  
  public void testLike() {
    Vector other = test.like();
    assertTrue("not like", other instanceof WritableSparseVector);
    assertEquals("cardinality", test.cardinality(), other.cardinality());
  }

  public void testLikeN() {
    Vector other = test.like(8);
    assertTrue("not like", other instanceof WritableSparseVector);
    assertEquals("cardinality", 8, other.cardinality());
  }
}
