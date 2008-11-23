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
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.GenericsUtil;

import redpoll.core.LabeledWritableVector;
import redpoll.core.WritableVector;

public class KMeansReducer extends MapReduceBase implements
        Reducer<Text, WritableVector, Text, WritableVector> {

  private  HashMap<String, Cluster> clusters = null;

  public void reduce(Text key, Iterator<WritableVector> values,
                     OutputCollector<Text, WritableVector> output, Reporter reporter) throws IOException {
    Cluster cluster = clusters.get(key.toString());
    while (values.hasNext()) {
      LabeledWritableVector value = (LabeledWritableVector) values.next();
      int count = Integer.parseInt(value.getLabel().toString());
      cluster.addPoints(count, value.getVector());
    }
    // force convergence calculation
    cluster.computeConvergence();
    output.collect(new Text(cluster.getIdentifier()), cluster.computeCentroid());
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
        Cluster cluster = new Cluster(entry.getKey(), entry.getValue());
        cluster.addPoint(cluster.getCenter());
        clusters.put(entry.getKey(), cluster);
      }
    } catch (IOException exp) {
      exp.printStackTrace();
    }
  }

}
