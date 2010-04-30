/* Copyright 2004-2010 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.batch.integration.chunk.gridgain;

import java.io.Serializable;
import java.util.List;

import org.apache.log4j.Logger;
import org.springframework.batch.item.ItemWriter;

/**
 * 
 * @author Alois Cochard
 *
 */
public class ConsoleWriter implements ItemWriter<Object>, Serializable {

	private static final long serialVersionUID = 1L;
	private static final Logger logger = Logger.getLogger(ConsoleWriter.class);

	@Override
	public void write(List<? extends Object> items) throws Exception {
		for (Object item : items) {
			logger.info("item: "+ item.toString());
		}
	}

}
