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
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.lucene.util.OpenBitSet;
import org.apache.mahout.matrix.SparseVector;

/**
 * Reducer to create vsm of documents.
 * @author Jeremy Chow(coderplay@gmail.com)
 */
public class TfIdfReducer extends MapReduceBase implements Reducer<LongWritable, TfIdfWritable, LongWritable, TfIdfWritable>{
  
  private int totalTerms;
  
  public void reduce(LongWritable key, Iterator<TfIdfWritable> values,
      OutputCollector<LongWritable, TfIdfWritable> output, Reporter reporter)
      throws IOException {
    SparseVector tfIdfVector = new SparseVector(totalTerms);
    OpenBitSet bits = new OpenBitSet(totalTerms);
    
    while (values.hasNext()) {
      ElementWritable value = (ElementWritable) values.next().get();
      int index = value.index(); double tfIdf = value.get();
      tfIdfVector.setQuick(index, tfIdf);
      if(tfIdf > 0)
        bits.fastSet(index);
    }
    
    // tf-idf style vector
    output.collect(key, new TfIdfWritable(tfIdfVector));
    // bitset style
    output.collect(key, new TfIdfWritable(new OpenBitSetWritable(bits)));
  }
  
  @Override
  public void configure(JobConf job) {
    totalTerms = job.getInt("redpoll.text.terms.num", 1024);
  }

}
