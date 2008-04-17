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

package redpoll.core;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * attributes
 * 
 * @author Jeremy Chow(coderplay@gmail.com)
 */
public class Attribute<T> {

	public static final String ATTRIBUTE_PATH_KEY = "redpoll.attribute.path";
	public static final String SPLIT_PATTERN = ",*\\s+\\{*|\\}";

	/* The attribute's name. */
	private String name;
	/* The attribute's value list */
	private List<T> values;

	public Attribute(String attributeName, List<T> attributeValues) {
		name = attributeName;
		values = attributeValues;
	}

	public Attribute(String attributeName) {
		name = attributeName;
		values = new ArrayList<T>();
	}

	public Attribute() {
		values = new ArrayList<T>();
	}

	public void addValue(T value) {
		values.add(value);
	}
	
	public Iterator<T> iterator() {
		return values.iterator();
	}
	
	public String getName() {
		return name;
	}

}
