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
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.GenericsUtil;

/**
 * Mapper to create vsm of documents.
 * @author Jeremy Chow(coderplay@gmail.com)
 */
public class TfIdfMapper extends MapReduceBase implements
  Mapper<Text, TfArrayWritable, Text, TfIdfWritable> {
  private static final Log log = LogFactory.getLog(TfIdfMapper.class
      .getName());
  
  private HashMap<String, Integer> terms = null;
  private int toalDocNum;
  
  public void map(Text key, TfArrayWritable value,
      OutputCollector<Text, TfIdfWritable> output, Reporter reporter)
      throws IOException {
    Writable[] tfs = value.get();
    double df = (double) tfs.length;            // document frequcy
    Integer index = terms.get(key.toString());  // index of term vector
    
    for (int i = 0; i < tfs.length; i++) {
      TfWritable tfWritable = (TfWritable) tfs[i];
      double tf = 1.0 + Math.log(1.0 + Math.log((double) tfWritable.getTf()));
      double idf = Math.log((1 + toalDocNum) / df);
      double tfIdf = tf * idf;

      output.collect(new Text(tfWritable.getDocumentId()),
          new TfIdfWritable(new ElementWritable(index.intValue(), tfIdf)));
    }
  }
  


  @Override
  public void configure(JobConf job) {
    try {
      terms = new HashMap<String, Integer>();
      
      DefaultStringifier<HashMap<String, Integer>> mapStringifier = 
          new DefaultStringifier<HashMap<String, Integer>>(
                job, GenericsUtil.getClass(terms));
      
      String docFreqString = job.get("redpoll.text.terms");
      terms = mapStringifier.fromString(docFreqString);
      toalDocNum = job.getInt("redpoll.docs.num", 1024);
    } catch (IOException exp) {
      exp.printStackTrace();
    }
  }
}
