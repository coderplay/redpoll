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

package redpoll.cluster.em;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * @author Jeremy Chow(coderplay@gmail.com)
 * 
 */
public class EMMapper extends MapReduceBase implements
		Mapper<WritableComparable, Text, Text, Text> {

	/** Constant for normal distribution. */
	private static double m_normConst = Math.log(Math.sqrt(2 * Math.PI));

	public void map(WritableComparable arg0, Text arg1,
			OutputCollector<Text, Text> arg2, Reporter arg3) throws IOException {

	}

	/**
	 * Density function of normal distribution.
	 * 
	 * @param x input value
	 * @param mean mean of distribution
	 * @param stdDev standard deviation of distribution
	 * @return the density
	 */
	private double logNormalDensity(double x, double mean, double stdDev) {
		double diff = x - mean;
		return -(diff * diff / (2 * stdDev * stdDev)) - m_normConst
				- Math.log(stdDev);
	}

}
