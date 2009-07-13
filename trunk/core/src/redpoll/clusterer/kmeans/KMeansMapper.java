/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package redpoll.clusterer.kmeans;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.GenericsUtil;

import redpoll.core.WritableVector;

public class KMeansMapper extends MapReduceBase implements
        Mapper<Text, WritableVector, Text, WritableVector> {

  private List<Cluster> clusters;

  public void map(Text key, WritableVector value,
                  OutputCollector<Text, WritableVector> output, Reporter reporter) throws IOException {
    Cluster.emitPointToNearestCluster(value, clusters, output);
  }


  @Override
  public void configure(JobConf job) {
    Cluster.configure(job);
    
    try {
      HashMap<String, WritableVector> centers = new HashMap<String, WritableVector>();
      DefaultStringifier<HashMap<String, WritableVector>> stringifier = 
          new DefaultStringifier<HashMap<String, WritableVector>>(
                job, GenericsUtil.getClass(centers));
      String centersString = job.get("redpoll.clusterer.kmeans.centers");
      centers = stringifier.fromString(centersString);
      
      for(Map.Entry<String, WritableVector> entry : centers.entrySet()) {
        clusters.add(new Cluster(entry.getKey(), entry.getValue()));
      }
    } catch (IOException exp) {
      exp.printStackTrace();
    }
  }
}
