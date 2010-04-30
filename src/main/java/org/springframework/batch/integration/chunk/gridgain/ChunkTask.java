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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.gridgain.grid.GridException;
import org.gridgain.grid.GridJob;
import org.gridgain.grid.GridJobAdapter;
import org.gridgain.grid.GridJobResult;
import org.gridgain.grid.GridTaskSplitAdapter;
import org.gridgain.grid.logger.GridLogger;
import org.gridgain.grid.resources.GridLoggerResource;
import org.springframework.batch.core.step.item.Chunk;
import org.springframework.batch.core.step.item.ChunkProcessor;
import org.springframework.batch.integration.chunk.ChunkRequest;
import org.springframework.batch.integration.chunk.ChunkResponse;

/**
 * 
 * @author Alois Cochard
 * 
 */

// TODO [acochard] Is [Split] usefull ? no ...
public class ChunkTask<T> extends GridTaskSplitAdapter<ChunkRequest<T>, ChunkResponse> {

	/**
	 * Default serial version UID.
	 */
	private static final long serialVersionUID = 1L;

	@GridLoggerResource
	private final GridLogger log = null;

	private ChunkProcessor<T> processor;

	@Override
	public ChunkResponse reduce(final List<GridJobResult> results) throws GridException {
		assert results.size() == 1;
		return results.get(0).getData();
	}

	public void setProcessor(ChunkProcessor<T> processor) {
		this.processor = processor;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected Collection<? extends GridJob> split(final int gridSize, final ChunkRequest<T> request)
	throws GridException {
		List<GridJob> jobs = new ArrayList<GridJob>();

		ChunkTaskParameters parameters = new ChunkTaskParameters(this.processor, request);

		// Every job gets its own word as an argument.
		jobs.add(new GridJobAdapter<ChunkTaskParameters>(parameters) {

			/**
			 * Default serial version UID.
			 */
			private static final long serialVersionUID = 1L;

			public Serializable execute() {
				ChunkTaskParameters parameters = getArgument();
				Exception exception = null;

				try {
					parameters.getProcessor().process(parameters.getRequest().getStepContribution(),
							new Chunk<T>(parameters.getRequest().getItems()));
				} catch (Exception e) {
					log.error("Unable to process chunk", e);
					exception = e;
				}

				ChunkResponse response;
				if (exception == null) {
					response = new ChunkResponse(true, parameters.getRequest().getJobId(), parameters.getRequest()
							.getStepContribution());
				} else {
					response = new ChunkResponse(false, parameters.getRequest().getJobId(), parameters.getRequest()
							.getStepContribution(), exception.getMessage());
				}
				return response;
			}
		});

		return jobs;
	}

	private class ChunkTaskParameters implements Serializable {
		private static final long serialVersionUID = 1L;
		private final ChunkProcessor<T> processor;
		private final ChunkRequest<T> request;

		ChunkTaskParameters(final ChunkProcessor<T> processor, final ChunkRequest<T> request) {
			this.processor = processor;
			this.request = request;
		}

		public ChunkProcessor<T> getProcessor() {
			return processor;
		}

		public ChunkRequest<T> getRequest() {
			return request;
		}

	}
}
