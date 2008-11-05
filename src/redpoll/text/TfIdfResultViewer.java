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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.mahout.matrix.SparseVector;

/**
 * Vector space model result viewer, usage:
 * <p>
 * bin/hadoop jar redpoll-*.jar redpoll.text.TfIdfResultViewer outputDir
 * @author Jeremy Chow(coderplay@gmail.com)
 */
public class TfIdfResultViewer {

  private static Configuration conf = new Configuration();
  
  public static void main(String[] args) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    Path path = new Path(args[0] + "/tf-idf/part-00000");
    SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
    LongWritable key = new LongWritable();
    SparseVector value = new SparseVector();
    int counter = 0;
    while ((reader.next(key, value))) {
      System.out.println(key.get() + value.asFormatString());
      counter ++;
    }
    System.out.println("result count:\t" + counter);
    reader.close();
  }
}