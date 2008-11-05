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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.util.GenericsUtil;

/**
 * The Driver which drives the Tf-Idf based vector space model generation.
 * @author Jeremy Chow(coderplay@gmail.com)
 */
public class TfIdfDriver {

  /**
   * Run the job
   * 
   * @param input the input pathname String
   * @param output the output pathname String
   */
  public static void runJob(String input, String output) throws IOException {
    JobClient client = new JobClient();
    JobConf conf = new JobConf(TfIdfDriver.class);

    FileSystem fs = FileSystem.get(conf);
    Path inPath = new Path(input + "/tf");
    FileInputFormat.setInputPaths(conf, inPath);
    Path outPath = new Path(output);
    FileOutputFormat.setOutputPath(conf, outPath);

    conf.setMapperClass(TfIdfMapper.class);
    conf.setReducerClass(TfIdfReducer.class);
    //conf.setNumMapTasks(10);
    conf.setOutputKeyClass(LongWritable.class);
    conf.setOutputValueClass(TfIdfWritable.class);
    conf.setInputFormat(SequenceFileInputFormat.class);
    conf.setOutputFormat(TfIdfOutputFormat.class);

    conf.set("io.serializations",
             "org.apache.hadoop.io.serializer.JavaSerialization," + 
             "org.apache.hadoop.io.serializer.WritableSerialization");
    // serialize a term hashmap. Its key is the term , value is a term index of
    // the term vector.    
    Path dfpath = new Path(input + "/df/part-00000");
    SequenceFile.Reader reader = new SequenceFile.Reader(fs, dfpath, conf);
    Text key = new Text();
    IntWritable value = new IntWritable();
    HashMap<String, Integer> termMap = new HashMap<String, Integer>();
    int index = 0;
    while ((reader.next(key, value))) {
      String termString = key.toString();
      if (!termString.equals("redpoll.docs.num")) {
        termMap.put(key.toString(), index);
        index++;
      } else {
        System.out.println(":::::::::::::::::::::::::redpoll.text.terms.num:\t" + value.get());
        conf.setInt("redpoll.docs.num", value.get());
      }
    }
    reader.close();
    DefaultStringifier<HashMap<String, Integer>> mapStringifier = new DefaultStringifier<HashMap<String, Integer>>(
        conf, GenericsUtil.getClass(termMap));
    String termMapString = mapStringifier.toString(termMap);
    conf.setInt("redpoll.text.terms.num", index); // number of terms
    conf.set("redpoll.text.terms", termMapString);

    client.setConf(conf);
    JobClient.runJob(conf);
  }

  /**
   * for test
   */
  public static void main(String[] args) throws IOException {
    String input = args[0];
    String output = args[1];
    runJob(input, output);
  }
}
