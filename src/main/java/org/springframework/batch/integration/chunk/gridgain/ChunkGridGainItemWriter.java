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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.gridgain.grid.Grid;
import org.gridgain.grid.GridException;
import org.gridgain.grid.GridTaskFuture;
import org.gridgain.grid.GridTaskTimeoutException;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.listener.StepExecutionListenerSupport;
import org.springframework.batch.integration.chunk.AsynchronousFailureException;
import org.springframework.batch.integration.chunk.ChunkRequest;
import org.springframework.batch.integration.chunk.ChunkResponse;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemWriter;
import org.springframework.util.Assert;

/**
 * 
 * @author Alois Cochard
 * 
 */
public class ChunkGridGainItemWriter<T> extends StepExecutionListenerSupport implements ItemWriter<T>, ItemStream {

    static final String ACTUAL = ChunkGridGainItemWriter.class.getName() + ".ACTUAL";

    static final String EXPECTED = ChunkGridGainItemWriter.class.getName() + ".EXPECTED";

    private static final long DEFAULT_THROTTLE_LIMIT = 6;

    private static final Log logger = LogFactory.getLog(ChunkGridGainItemWriter.class);

    private final int DEFAULT_MAX_WAIT_TIMEOUTS = 40;

    private List<GridTaskFuture<ChunkResponse>> futures;

    private Grid grid;

    private final LocalState localState = new LocalState();

    private int maxWaitTimeouts = DEFAULT_MAX_WAIT_TIMEOUTS;

    private long throttleLimit = DEFAULT_THROTTLE_LIMIT;

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        if (!(stepExecution.getStatus() == BatchStatus.COMPLETED)) {
            return ExitStatus.EXECUTING;
        }
        long expecting = localState.getExpecting();
        boolean timedOut;
        try {
            logger.debug("Waiting for results in step listener...");
            timedOut = !waitForResults();
            logger.debug("Finished waiting for results in step listener.");
        } catch (RuntimeException e) {
            logger.debug("Detected failure waiting for results in step listener.", e);
            stepExecution.setStatus(BatchStatus.FAILED);
            return ExitStatus.FAILED.addExitDescription(e.getClass().getName() + ": " + e.getMessage());
        }
        if (timedOut) {
            stepExecution.setStatus(BatchStatus.FAILED);
            throw new ItemStreamException("Timed out waiting for back log at end of step");
        }
        return ExitStatus.COMPLETED.addExitDescription("Waited for " + expecting + " results.");
    }

    @Override
    public void beforeStep(StepExecution stepExecution) {
        localState.setStepExecution(stepExecution);
        // TODO [acochard] is it correct to allocate list here ?
        futures = new ArrayList<GridTaskFuture<ChunkResponse>>();
    }

    public void close() throws ItemStreamException {
        localState.reset();
    }

    public void open(ExecutionContext executionContext) throws ItemStreamException {
        if (executionContext.containsKey(EXPECTED)) {
            localState.expected = executionContext.getLong(EXPECTED);
            localState.actual = executionContext.getLong(ACTUAL);
            if (!waitForResults()) {
                throw new ItemStreamException("Timed out waiting for back log on open");
            }
        }
    }

    public void setGrid(Grid grid) {
        this.grid = grid;
    }

    /**
     * The maximum number of times to wait at the end of a step for a non-null
     * result from the remote workers. This is a multiplier on the receive
     * timeout set separately on the gateway. The ideal value is a compromise
     * between allowing slow workers time to finish, and responsiveness if there
     * is a dead worker. Defaults to 40.
     * 
     * @param maxWaitTimeouts
     *            the maximum number of wait timeouts
     */
    public void setMaxWaitTimeouts(int maxWaitTimeouts) {
        this.maxWaitTimeouts = maxWaitTimeouts;
    }

    /**
     * Public setter for the throttle limit. This limits the number of pending
     * requests for chunk processing to avoid overwhelming the receivers.
     * 
     * @param throttleLimit
     *            the throttle limit to set
     */
    public void setThrottleLimit(long throttleLimit) {
        this.throttleLimit = throttleLimit;
    }

    public void update(ExecutionContext executionContext) throws ItemStreamException {
        executionContext.putLong(EXPECTED, localState.expected);
        executionContext.putLong(ACTUAL, localState.actual);
    }

    public void write(List<? extends T> items) throws Exception {

        // Block until expecting <= throttle limit
        while (localState.getExpecting() > throttleLimit) {
            getNextResult();
        }

        if (!items.isEmpty()) {
            logger.debug("Dispatching chunk: " + items);
            ChunkRequest<T> request = new ChunkRequest<T>(items, localState.getJobId(), localState
                    .createStepContribution());

            // Create & launch task
            ChunkTask<T> task = new ChunkTask<T>();
            GridTaskFuture<ChunkResponse> future = grid.<ChunkRequest<T>, ChunkResponse> execute(task, request);
            futures.add(future);

            localState.expected++;

        }

    }

    /**
     * Get the next result if it is available (within the timeout specified in
     * the gateway), otherwise do nothing.
     * 
     * @throws AsynchronousFailureException
     *             If there is a response and it contains a failed chunk
     *             response.
     * 
     * @throws IllegalStateException
     *             if the result contains the wrong job instance id (maybe we
     *             are sharing a channel and we shouldn't be)
     */
    private void getNextResult() {
        ChunkResponse payload = null;

        // Retrieve first finished task found
        Iterator<GridTaskFuture<ChunkResponse>> iterator = futures.iterator();
        while (iterator.hasNext()) {
            GridTaskFuture<ChunkResponse> future = iterator.next();
            if (future.isDone()) {
                try {
                    payload = future.get();
                } catch (GridTaskTimeoutException e) {
                    // TODO [acochard] handle that correctly
                    e.printStackTrace();
                } catch (GridException e) {
                    // TODO [acochard] handle that correctly
                    e.printStackTrace();
                }
                iterator.remove();
            }
        }

        if (payload != null) {
            Long jobInstanceId = payload.getJobId();
            Assert.state(jobInstanceId != null, "Message did not contain job instance id.");
            Assert.state(jobInstanceId.equals(localState.getJobId()), "Message contained wrong job instance id ["
                    + jobInstanceId + "] should have been [" + localState.getJobId() + "].");
            localState.actual++;
            // TODO: apply the skip count
            if (!payload.isSuccessful()) {
                throw new AsynchronousFailureException("Failure or interrupt detected in handler: "
                        + payload.getMessage());
            }
        }
    }

    /**
     * Wait until all the results that are in the pipeline come back to the
     * reply channel.
     * 
     * @return true if successfully received a result, false if timed out
     */
    private boolean waitForResults() {
        int count = 0;
        int maxCount = maxWaitTimeouts;
        while (localState.getExpecting() > 0 && count++ < maxCount) {
            getNextResult();
        }
        return count < maxCount;
    }

    private static class LocalState {
        private long actual;

        private long expected;

        private StepExecution stepExecution;

        public StepContribution createStepContribution() {
            return stepExecution.createStepContribution();
        }

        public long getExpecting() {
            return expected - actual;
        }

        public Long getJobId() {
            return stepExecution.getJobExecution().getJobId();
        }

        public void reset() {
            expected = actual = 0;
        }

        public void setStepExecution(StepExecution stepExecution) {
            this.stepExecution = stepExecution;
        }
    }

}