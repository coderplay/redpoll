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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

import redpoll.core.Attribute;

/**
 * Class for a Naive Bayes classifier. <br/>
 * 
 * @author Jeremy Chow(coderplay@gmail.com)
 * 
 */
public class NaiveBayesDriver {

	/**
	 * main entry point
	 * 
	 * @param args should contain the following arguments: <p>
	 * 		args[0] the directory pathname for input data <br/> 
	 * 		args[1] the directory pathname for output data <br/> 
	 *		args[2] the directory pathname for attributes informations of the input data.<br/>
	 * 		args[3] classIndex index of class attribute (default: last). <br/>
	 */
	public static void main(String[] args) {
		String input = args[0];
		String output = args[1];
		String atrributesIn = args[2];
		String classIndex = args[3];
		runJob(input, output, atrributesIn, classIndex);
	}

	/**
	 * Run the job using supplied arguments
	 * 
	 * @param input the directory pathname for input data
	 * @param output the directory pathname for output statistical data
	 * @param attributesIn the directory pathname for attributes informations of the input data
	 * @param classIndex index of class attribute (default: last).
	 */
	public static void runJob(String input, String output, String attributesIn,
			String classIndex) {
		try {
			JobConf conf = new JobConf(NaiveBayesDriver.class);
			Path outPath = new Path(output);
			FileSystem fs = FileSystem.get(conf);
			if (fs.exists(outPath)) {
				fs.delete(outPath);
			}
			fs.mkdirs(outPath);

			// run a naive bayes classifier
			System.out.println("Running naive bayes classfier");
			runClassifier(input, output + "/part-00000", attributesIn, classIndex);

		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Run a naive bayes classifier using supplied arguments
	 * 
	 * @param input the directory pathname for input data
	 * @param output the directory pathname for output statistical data
	 * @param attributesIn the directory pathname for attributes informations of the input data
	 * @param classIndex index of class attribute (default: last).
	 */
	public static void runClassifier(String input, String output,
			String attributesIn, String classIndex) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(NaiveBayesDriver.class);

		conf.setJobName("naivebayes");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(LongWritable.class);

		conf.setInputPath(new Path(input));
		Path outPath = new Path(output);
		conf.setOutputPath(outPath);

		conf.setMapperClass(NaiveBayesMapper.class);
		conf.setCombinerClass(NaiveBayesReducer.class);
		conf.setReducerClass(NaiveBayesReducer.class);
		
		conf.set(Attribute.ATTRIBUTE_PATH_KEY, attributesIn);
		conf.set(NaiveBayes.CLASS_INDEX_KEY, classIndex);

		client.setConf(conf);
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
