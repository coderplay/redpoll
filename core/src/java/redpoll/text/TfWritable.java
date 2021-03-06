/**
 *  Licensed to the Apache Software Foundation (ASF) under one
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

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * term frequency
 * @author Jeremy Chow(coderplay@gmail.com)
 */
public class TfWritable implements Writable {

  private Text documentId;
  private IntWritable tf;

  public TfWritable() {
    documentId = new Text();
    tf = new IntWritable();
  }
  
  public TfWritable(String documentId, int tf) {
    set(documentId, tf);
  }
  
  public TfWritable(Text documentId,  IntWritable tf) {
    set(documentId, tf);
  }
  
  public void set(String documentId,  int tf) {
    set(new Text(documentId), new IntWritable(tf));
  }
  
  public void set(Text documentId,  IntWritable tf) {
    this.documentId = documentId;
    this.tf = tf;
  }

  public void readFields(DataInput in) throws IOException {
    documentId.readFields(in);
    tf.set(WritableUtils.readVInt(in));
  }

  public void write(DataOutput out) throws IOException {
    documentId.write(out);
    WritableUtils.writeVInt(out, tf.get());
  }
  
  public String toString() {
    return documentId + ":" + tf;
  }

  public String getDocumentId() {
    return documentId.toString();
  }

  public int getTf() {
    return tf.get();
  }


}