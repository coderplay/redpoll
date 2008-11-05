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
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

/**
 * A class provides for doing words segmenation and counting term TFs and DFs.<p>
 * in: key is document id, value is a document instance. <br>
 * output:
 * <li>key is term, value is frequency of this term. </li>
 * <li>key is term, value is one weight of term document frequency.</li>
 * @author Jeremy Chow(coderplay@gmail.com)
 */
public class TermMapper extends MapReduceBase implements
    Mapper<LongWritable, Document, Text, TermWritable> {
  private static final Log log = LogFactory.getLog(TermMapper.class
      .getName());
  
  private Analyzer analyzer = null;
   
  /* frequency weight for document title */
  private Integer titleWeight = new Integer(2);
  /* frequency weight for document content */
  private Integer contentWeight = new Integer(1);
 
  public void map(LongWritable key, Document value,
      OutputCollector<Text, TermWritable> output, Reporter reporter)
      throws IOException {   
    Map<String, Integer> wordMap = new HashMap<String, Integer>(1024);
    collectWords(value.getTitle(), wordMap, titleWeight);
    collectWords(value.getContent(), wordMap, contentWeight);
    
    for(Map.Entry<String, Integer> entry: wordMap.entrySet()) {
      Text term = new Text(entry.getKey());
      IntWritable freq = new IntWritable(entry.getValue());
      TfWritable tf = new TfWritable(key, freq);
      
      // <term, <documentId,tf>>
      output.collect(term, new TermWritable(tf)); // wrap then collect
    }
    
    // for counting total documents number
    TfWritable blank = new TfWritable();
    output.collect(new Text("redpoll.docs.num"), new TermWritable(blank));
  }
    
  private void collectWords(String text, Map<String, Integer> words,
      Integer weight) throws IOException {
    // do words segmentation
    TokenStream ts = analyzer.tokenStream("dummy", new StringReader(text));
    Token token = new Token();
    while ((token = ts.next(token)) != null) {
      String termString = new String(token.termBuffer(), 0, token.termLength());
      Integer value = words.get(termString);
      if (value == null)
        words.put(termString, weight);
      else
        words.put(termString, value + weight);
    }
  }
  
  @Override
  public void configure(JobConf job) {
    String analyzerName = job.get("redpoll.text.analyzer");
    try {
      if (analyzerName != null)
        analyzer = (Analyzer) Class.forName(analyzerName).newInstance();
    } catch (Exception excp) {
      excp.printStackTrace();
    }
    if (analyzer == null)
      analyzer = new StandardAnalyzer();
  }

}
