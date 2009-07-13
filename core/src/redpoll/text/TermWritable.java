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

package redpoll.text;

import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.Writable;

/**
 * Generic Writable class for terms.
 * @author Jeremy Chow(coderplay@gmail.com)
 */
public class TermWritable extends GenericWritable {
  private static Class<? extends Writable>[] CLASSES = null;

  
  static {
    CLASSES = (Class<? extends Writable>[]) new Class[] {
        redpoll.text.TfArrayWritable.class,
        org.apache.hadoop.io.IntWritable.class, 
        redpoll.text.TfWritable.class };
  }

  public TermWritable() {
  }

  public TermWritable(Writable instance) {
    set(instance);
  }

  @Override
  protected Class<? extends Writable>[] getTypes() {
    return CLASSES;
  }
}
