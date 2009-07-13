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

package redpoll.examples.sogou;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

import redpoll.text.TermMapper;
import redpoll.text.TermOutputFormat;
import redpoll.text.TermReducer;
import redpoll.text.TermWritable;

/**
 * @author Jeremy Chow(coderplay@gmail.com)
 */
public class SogouTermDriver {
  private static final Log LOG = LogFactory.getLog(SogouTermDriver.class
      .getName());

  public static void runJob(String input, String output, String analyzerName, int dfLimit)
      throws IOException {

    JobClient client = new JobClient();
    JobConf conf = new JobConf(SogouTermDriver.class);

    FileSystem fs = FileSystem.get(conf);
    Path outPath = new Path(output);
    if (fs.exists(outPath)) {
      fs.delete(outPath, true);
    }
    
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(TermWritable.class);
    FileInputFormat.setInputPaths(conf, new Path(input));
    FileOutputFormat.setOutputPath(conf, outPath);

    conf.set("redpoll.text.analyzer", analyzerName);
    conf.setInt("redpoll.text.df.limit", dfLimit);

    conf.setMapperClass(TermMapper.class);
    conf.setReducerClass(TermReducer.class);
    conf.setInputFormat(SogouInputFormat.class);
    conf.setOutputFormat(TermOutputFormat.class);
    client.setConf(conf);

    try {
      JobClient.runJob(conf);
    } catch (IOException e) {
      LOG.error(e.toString());
    }
  }

  public static void main(String[] args) throws IOException {
    String input = args[0];
    String output = args[1];
    String analyzerName = args[2];
    int dfLimit = Integer.parseInt(args[3]);
    SogouTermDriver.runJob(input, output, analyzerName, dfLimit);
  }

}
