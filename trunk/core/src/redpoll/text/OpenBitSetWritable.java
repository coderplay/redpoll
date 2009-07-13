/* Licensed to the Apache Software Foundation (ASF) under one
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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.lucene.util.OpenBitSet;

/**
 * @author Jeremy Chow(coderplay@gmail.com)
 */
public class OpenBitSetWritable implements Writable{

  private OpenBitSet bitset;
  
  public OpenBitSetWritable(OpenBitSet bs) {
    this.bitset = bs;
  }
  
  public OpenBitSetWritable(long[] bits, int wordsNum) {
    this.bitset = new OpenBitSet(bits, wordsNum);
  }
  
  public void fastSet(int index) {
    bitset.set(index);
  }
  
  public boolean fastGet(int index) {
    return bitset.fastGet(index);
  }
  
  public OpenBitSet get() {
    return bitset;
  }
  
  public void readFields(DataInput in) throws IOException {
    int wordsNum = WritableUtils.readVInt(in);
    long[] bits = new long[wordsNum];
    for (int i = 0; i < wordsNum; i++) {
      bits[i] = in.readLong();
    }
  }

  public void write(DataOutput out) throws IOException {
    long[] bits = bitset.getBits();
    WritableUtils.writeVInt(out, bits.length);
    for (int i = 0; i < bits.length; i++) {
      out.writeLong(bits[i]);
    }
  }

}
