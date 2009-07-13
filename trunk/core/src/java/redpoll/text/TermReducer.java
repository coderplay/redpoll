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
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * Collects a words list buffer and caculates the df for terms.
 * @author Jeremy Chow(coderplay@gmail.com)
 */
public class TermReducer extends MapReduceBase implements Reducer<Text, TermWritable, Text, TermWritable> {
  
  private static final Log log = LogFactory.getLog(TermReducer.class.getName());
  
  private int dfLimit;
  public void reduce(Text key, Iterator<TermWritable> values,
      OutputCollector<Text, TermWritable> output, Reporter reporter)
      throws IOException {
    
    ArrayList<TfWritable> tfs = new ArrayList<TfWritable>();
    
    while (values.hasNext()) {
      TfWritable value = (TfWritable) values.next().get();
      tfs.add((TfWritable)value);  
    }
    
    int df = tfs.size();
    TfWritable writables[] = new TfWritable[df];
    ArrayWritable aw = new TfArrayWritable(tfs.toArray(writables));
    
    if(df > dfLimit) {
      // if not stands for documents' total number, then ouput term's tf vector
      if(!key.toString().equals("redpoll.docs.num"))
        output.collect(key, new TermWritable(aw)); // wrap again
      output.collect(key, new TermWritable(new IntWritable(df)));
    }
  }
  
  @Override
  public void configure(JobConf job) {
    dfLimit = job.getInt("redpoll.text.df.limit", 3);
  }

}
