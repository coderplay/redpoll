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

package redpoll.classifier.naivebayes;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import redpoll.core.Attribute;

/**
 * Mapper for naive bayes algorithm. It reads attributes from dfs and delivers
 * each instance into a class.
 * 
 * @author Jeremy Chow(coderplay@gmail.com)
 */
public class NaiveBayesMapper<K extends WritableComparable> extends MapReduceBase implements
		Mapper<K, Text, Text, LongWritable> {

	private int classIndex;
	private List<Attribute<String>> attributes;

	public void map(K key, Text values,
			OutputCollector<Text, LongWritable> output, Reporter reporter)
			throws IOException {
		// TODO: deal with lines of literal data, and collect them into
		// specific classes corresponding their values.
		String line = values.toString();
		/* illegal line */
		if (line.trim().equals("") || line.startsWith("@"))
			return;
		String[] splits = line.split(",");
		output.collect(new Text(splits[classIndex]), new LongWritable(1));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapred.MapReduceBase#configure(org.apache.hadoop.mapred.JobConf)
	 */
	@Override
	public void configure(JobConf job) {
		super.configure(job);

		String classPath = job.get(Attribute.ATTRIBUTE_PATH_KEY);
		attributes = new ArrayList<Attribute<String>>();

		try {
			FileSystem fs = FileSystem.get(job);
			Path path = new Path(classPath + "/attributes");
			BufferedReader reader = new BufferedReader(new InputStreamReader(fs
					.open(path)));
			String line;
			/* parse a line into an string attribute */
			while ((line = reader.readLine()) != null) {
				String[] splits = line.split(",*\\s+\\{*|\\}");
				if (splits.length <= 2 || !splits[0].equals("@attribute"))
					continue;
				Attribute<String> attribute = new Attribute<String>(splits[1]);
				for (int i = 2; i < splits.length; i++)
					attribute.addValue(splits[i]);
				attributes.add(attribute);
			}
			reader.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		classIndex = job.getInt(NaiveBayes.CLASS_INDEX_KEY, 0);

		// /* dump result */
		// for(Iterator<Attribute<String>> it = attributes.iterator();
		// it.hasNext(); ) {
		// Attribute<String> att = it.next();
		// System.out.println("attribute name : " + att.getName());
		// for(Iterator<String> iter = att.iterator(); iter.hasNext(); ) {
		// System.out.println(iter.next());
		// }
		// }
	}
}
