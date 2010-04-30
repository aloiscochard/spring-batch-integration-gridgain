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

import org.apache.log4j.Logger;
import org.springframework.batch.item.ItemProcessor;

/**
 * 
 * @author Alois Cochard
 *
 */
public class WordCounterProcessor implements ItemProcessor<String, Integer>, Serializable {

	private static final long serialVersionUID = 1L;
	private static final Logger logger = Logger.getLogger(WordCounterProcessor.class);
	@Override
	public Integer process(String item) throws Exception {
		if (logger.isDebugEnabled()) {
			logger.debug("Counting words in: " + item);
		}
		return item.split(" ").length;
	}

}
