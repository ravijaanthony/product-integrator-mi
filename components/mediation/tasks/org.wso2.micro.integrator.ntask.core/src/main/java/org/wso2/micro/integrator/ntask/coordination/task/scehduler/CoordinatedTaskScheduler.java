/*
 * Copyright (c) 2020, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.micro.integrator.ntask.coordination.task.scehduler;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.commons.util.MiscellaneousUtil;
import org.apache.synapse.core.SynapseEnvironment;
import org.apache.synapse.inbound.InboundEndpoint;
import org.apache.synapse.message.processor.MessageProcessor;
import org.apache.synapse.task.TaskDescription;
import org.apache.synapse.task.TaskDescriptionRepository;
import org.wso2.micro.integrator.coordination.ClusterCoordinator;
import org.wso2.micro.integrator.core.util.MicroIntegratorBaseUtils;
import org.wso2.micro.integrator.ntask.common.TaskException;
import org.wso2.micro.integrator.ntask.coordination.TaskCoordinationException;
import org.wso2.micro.integrator.ntask.coordination.task.ClusterCommunicator;
import org.wso2.micro.integrator.ntask.coordination.task.CoordinatedTask;
import org.wso2.micro.integrator.ntask.coordination.task.resolver.TaskLocationResolver;
import org.wso2.micro.integrator.ntask.coordination.task.store.TaskStore;
import org.wso2.micro.integrator.ntask.coordination.task.store.connector.RDMBSConnector;
import org.wso2.micro.integrator.ntask.coordination.task.store.cleaner.TaskStoreCleaner;
import org.wso2.micro.integrator.ntask.core.TaskUtils;
import org.wso2.micro.integrator.ntask.core.impl.standalone.ScheduledTaskManager;
import org.wso2.micro.integrator.ntask.core.internal.DataHolder;
import org.wso2.micro.integrator.ntask.core.internal.TaskHandlingConfigUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Scheduler class, which runs periodically to retrieve all the scheduled tasks assigned to the node and schedule
 * them locally. Also if the running node is leader, it cleans up the task store and resolve the un assigned nodes.
 */
public class CoordinatedTaskScheduler implements Runnable {

    private static final Log LOG = LogFactory.getLog(CoordinatedTaskScheduler.class);

    private DataHolder dataHolder = DataHolder.getInstance();
    private TaskLocationResolver taskLocationResolver;
    private ClusterCoordinator clusterCoordinator = dataHolder.getClusterCoordinator();
    private TaskStore taskStore;
    private TaskStoreCleaner taskStoreCleaner;
    private int resolvingFrequency;
    private int resolveCount = 0;
    private ClusterCommunicator clusterCommunicator;
    private ScheduledTaskManager taskManager;
    private String localNodeId;
    private boolean isCoordinationStarted = true;

    /**
     * Runs best-effort observation writes outside the scheduler loop. The thread is daemon so it never
     * holds shutdown, and the executor is static/single-threaded so rejoin cycles do not create extra
     * writers or publish observations concurrently.
     */
    private static final ExecutorService OBSERVATION_DB_EXECUTOR = Executors.newSingleThreadExecutor(runnable
            -> {
        Thread thread = new Thread(runnable, "RunningTaskObservationDbWriter");
        thread.setDaemon(true);
        return thread;
    });

    /**
     * Last submitted observation write. If it is still running, the next scheduler cycle intentionally
     * skips publishing instead of queuing another DB write with data that may already be stale.
     */
    private static volatile Future<?> observationWrite;

    /**
     * Prevents repeatedly logging the same timing warning when the configured freshness window cannot fit
     * below the heartbeat retry interval.
     */
    private boolean freshnessWindowWarned = false;

    public CoordinatedTaskScheduler(ScheduledTaskManager taskManager, TaskStore taskStore,
                                    TaskLocationResolver taskLocationResolver, ClusterCommunicator connector) {
        this(taskManager, taskStore, taskLocationResolver, connector, null, 1);
    }

    public CoordinatedTaskScheduler(ScheduledTaskManager taskManager, TaskStore taskStore,
                                    TaskLocationResolver taskLocationResolver, ClusterCommunicator connector,
                                    TaskStoreCleaner cleaner, int frequency) {
        this.taskManager = taskManager;
        this.taskStore = taskStore;
        this.taskLocationResolver = taskLocationResolver;
        this.clusterCommunicator = connector;
        this.taskStoreCleaner = cleaner;
        this.resolvingFrequency = frequency;
        this.localNodeId = clusterCoordinator.getThisNodeId();
    }

    @Override
    public void run() {

        try {
            if (isCoordinationStarted && clusterCoordinator.isDuplicateNode()) {
                try {
                    LOG.warn("This node is a duplicate node. Hence waiting for the cluster to settle down.");
                    Thread.sleep(clusterCoordinator.getHeartbeatMaxRetryInterval());
                } catch (InterruptedException ex) {
                    // ignore
                }
            }
            isCoordinationStarted = false;
            pauseDeactivatedTasks();
            scheduleAssignedTasks(CoordinatedTask.States.ACTIVATED);
            checkInterrupted();
            if (clusterCoordinator.isLeader()) {
                // cleaning will run for each n times resolving frequency . ( n = 0,1,2 ... ).
                if (resolveCount % resolvingFrequency == 0) {
                    LOG.debug("This node is leader hence cleaning task store.");
                    taskStoreCleaner.clean();
                    resolveCount = 0;
                }
                LOG.debug("This node is leader hence resolving unassigned tasks.");
                addFailedTasks();
                resolveCount++;
                resolveUnassignedNotCompletedTasksAndUpdateStore();
                // Only the leader opens, escalates, and closes duplicate-execution episodes.
                detectDuplications();
            } else {
                LOG.debug("This node is not leader. Hence not cleaning task store or resolving un assigned tasks.");
            }
            // schedule all tasks assigned to this node and in state none
            scheduleAssignedTasks(CoordinatedTask.States.NONE);
            // Publish this node's local running set after scheduling decisions for this cycle are done.
            recordRunningTaskObservations();
            checkInterrupted();
        } catch (InterruptedException ie) {
            // Expected during becameUnresponsive — shutdownNow interrupted this polling iteration.
            // Not an error; cleanup runs in the finally block below.
            LOG.warn("Coordinated task scheduler iteration interrupted"
                    + "; exiting current iteration. Cleanup will run in finally block. ", ie);
        } catch (Throwable throwable) { // catching throwable to prohibit permanent stopping of the executor service.
            LOG.fatal("Unexpected error occurred while trying to schedule tasks.", throwable);
        } finally {
            // Backstop cleanup. If shutdownNow set the interrupt flag (TaskEventListener.becameUnresponsive
            // ran) OR dataHolder cleared the scheduler reference, drain anything this iteration may have added to
            // locallyRunningCoordinatedTasks before exiting. pauseAllLocallyRunningTasks is synchronized and idempotent —
            // serializes with the heartbeat thread's call and the second caller's snapshot is empty/residue.
            if (Thread.currentThread().isInterrupted() || DataHolder.getInstance().getTaskScheduler() == null) {
                try {
                    LOG.warn("Coordinated Task Scheduler is shutting down And Interrupt Detected. "
                            + "Pausing all locally running tasks as part of final cleanup.");
                    taskManager.pauseAllLocallyRunningTasks();
                } catch (Throwable t) {
                    LOG.error("Cleanup in finally failed.", t);
                }
            }
        }
    }

    /**
     * Publishes the tasks currently running on this node for monitoring and duplicate detection. This is
     * best-effort telemetry: failures are logged, but they must not stop or delay task scheduling.
     */
    private void recordRunningTaskObservations() {
        if (!TaskHandlingConfigUtils.isTaskMonitoringEnabled()) {
            // Monitoring is opt-in; leave the observation tables untouched when it is disabled.
            return;
        }
        // Keep DB turbulence away from the scheduler thread. At most one write is allowed in flight.
        Future<?> pending = observationWrite;
        if (pending != null && !pending.isDone()) {
            // The previous write has not completed yet. Drop this cycle instead of building a backlog of
            // observation writes that would be obsolete by the time they reach the database.
            return;
        }
        observationWrite = OBSERVATION_DB_EXECUTOR.submit(() -> {
            // Never let a telemetry failure kill the single writer thread; the next scheduler cycle can
            // try again if the database has recovered.
            try {
                // Capture the running set inside the writer, not before submit. If the writer was delayed,
                // this publishes a recent local view instead of a snapshot from an older scheduler cycle.
                List<String> running = taskManager.getLocallyRunningCoordinatedTasks();
                long cycleTime = System.currentTimeMillis();
                taskStore.recordObservations(localNodeId, running, cycleTime);
            } catch (Throwable t) {
                LOG.warn("Failed to record running task observations for duplication detection "
                        + "(coordination DB may be turbulent); will retry next cycle.");
            }
        });
    }

    /**
     * Maintains the duplicate execution view from fresh observations written by all nodes. The leader
     * opens an episode when the same task is seen on multiple nodes, marks it sustained if the overlap
     * lasts long enough, and closes it when the overlap disappears.
     */
    private void detectDuplications() {
        if (!TaskHandlingConfigUtils.isTaskMonitoringEnabled()) {
            // Monitoring is opt-in; without observations there is nothing useful to detect.
            return;
        }
        try {
            long now = System.currentTimeMillis();
            long freshnessWindow = resolveFreshnessWindowMs();
            Map<String, Set<String>> freshByTask =
                    taskStore.readFreshObservationsByTask(now - freshnessWindow);
            Set<String> currentDuplicates = new HashSet<>();
            for (Map.Entry<String, Set<String>> entry : freshByTask.entrySet()) {
                if (entry.getValue().size() >= 2) {
                    currentDuplicates.add(entry.getKey());
                }
            }

            long sustainedThreshold = 2L * clusterCoordinator.getHeartbeatMaxRetryInterval();
            Set<String> openTasks = new HashSet<>();
            for (RDMBSConnector.DuplicationEpisode episode : taskStore.getOpenDuplicationEpisodes()) {
                String task = episode.getTaskName();
                openTasks.add(task);
                if (currentDuplicates.contains(task)) {
                    // The overlap is still present. Escalate only after it survives the configured grace period.
                    if (episode.getSeverity() == null && (now - episode.getDetectedAt()) >= sustainedThreshold) {
                        taskStore.markDuplicationEpisodeSustained(task);
                        LOG.error("Coordinated task duplication SUSTAINED for " + (now - episode.getDetectedAt())
                                + "ms - task [" + task + "] running on nodes " + freshByTask.get(task)
                                + ". Likely concurrent execution / double processing; check node heartbeat "
                                + "configuration.");
                    }
                } else {
                    // The task no longer overlaps across nodes. Close the episode and classify it if needed.
                    taskStore.closeDuplicationEpisode(task, now);
                    String severity = episode.getSeverity() == null ? "TRANSIENT" : episode.getSeverity();
                    LOG.warn("Coordinated task duplication CLEARED (" + severity + ") - task [" + task + "] lasted "
                            + (now - episode.getDetectedAt()) + "ms.");
                }
            }

            // Open an episode for each duplicate task that was not already being tracked.
            Map<String, String> destinedByTask = null;
            for (String task : currentDuplicates) {
                if (openTasks.contains(task)) {
                    continue;
                }
                if (destinedByTask == null) {
                    destinedByTask = new HashMap<>();
                    for (CoordinatedTask coordinatedTask : taskStore.getAllTaskNames()) {
                        destinedByTask.put(coordinatedTask.getTaskName(), coordinatedTask.getDestinedNodeId());
                    }
                }
                Set<String> nodes = freshByTask.get(task);
                String destined = destinedByTask.get(task);
                taskStore.openDuplicationEpisode(task, String.join(",", nodes), destined, now, "UNEXPECTED");
                LOG.warn("Coordinated task duplication DETECTED - task [" + task + "] running on nodes " + nodes
                        + "; DB owner [" + destined + "]. A node is running a coordinated task it does not own "
                        + "(possible heartbeat / clock skew).");
            }
        } catch (Throwable t) {
            LOG.warn("Coordinated task duplication detection cycle failed; will retry next cycle.");
        }
    }

    /**
     * Returns the observation age that still counts as "currently running". The value normally stays
     * below the heartbeat retry interval so dead-node rows age out before reassignment. If configuration
     * makes that impossible, live-node visibility wins and a warning is logged once.
     *
     * @return freshness window in milliseconds
     */
    private long resolveFreshnessWindowMs() {
        int heartbeat = clusterCoordinator.getHeartbeatMaxRetryInterval();
        long window = TaskHandlingConfigUtils.computeDuplicationFreshnessWindowMs(heartbeat);
        if (heartbeat > 0 && window >= heartbeat && !freshnessWindowWarned) {
            LOG.warn("Duplication-detection freshness window (" + window + "ms) is not below the heartbeat "
                    + "retry interval (" + heartbeat + "ms); the heartbeat interval is configured very low, so "
                    + "a normal coordinated-task failover may briefly register as a transient duplicate. Raise "
                    + "the heartbeat interval for cleaner detection.");
            freshnessWindowWarned = true;
        }
        return window;
    }

    /**
     * Check if the task is interrupted. Cleanup is no longer done here — the finally block in run()
     * is the single ownership point so that the heartbeat thread (TaskEventListener.becameUnresponsive)
     * and the polling thread never iterate JmsConsumer.cleanup concurrently on the same consumer.
     */
    private void checkInterrupted() throws InterruptedException {

        if (Thread.currentThread().isInterrupted()) {
            Thread.currentThread().interrupt();
            throw new InterruptedException("Thread was interrupted By the TaskEventListener.becameUnresponsive.");
        }
    }

    /**
     * Pause ( stop execution ) the deactivated tasks.
     *
     * @throws TaskCoordinationException - when something goes wrong while retrieving tasks information from store.
     */
    private void pauseDeactivatedTasks() throws TaskCoordinationException {

        List<String> deactivatedTasks = taskStore.retrieveTaskNames(localNodeId, CoordinatedTask.States.DEACTIVATED);
        if (LOG.isDebugEnabled()) {
            deactivatedTasks.stream().map(
                    task -> "Task [" + task + "] retrieved in [" + CoordinatedTask.States.DEACTIVATED + "] state.")
                    .forEachOrdered(LOG::debug);
        }
        ScheduledTaskManager scheduledTaskManager = taskManager;
        List<String> pausedTasks = new ArrayList<>();
        List<String> deployedTasks = taskManager.getAllCoordinatedTasksDeployed();
        deactivatedTasks.forEach(task -> {
            if (deployedTasks.contains(task)) {
                try {
                    scheduledTaskManager.stopExecution(task);
                    pausedTasks.add(task);
                } catch (TaskException e) {
                    LOG.error("Error stopping the task [" + task + "]", e);
                }
            } else {
                LOG.info("The task [" + task + "] retrieved to be paused is not a deployed task "
                                 + "in this node or an invalid entry, hence ignoring it.");
            }
        });
        notifyOnPause(pausedTasks);
        taskStore.updateTaskState(pausedTasks, CoordinatedTask.States.PAUSED);
    }

    /**
     * Add failed tasks to the store.
     *
     * @throws TaskCoordinationException - When something goes wrong while retrieving all the tasks from store.
     */
    private void addFailedTasks() throws TaskCoordinationException {

        List<ScheduledTaskManager.TaskEntry> failedTasks = taskManager.getAdditionFailedTasks();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Following list of tasks were found in the failed list.");
            failedTasks.forEach(LOG::debug);
        }
        for (ScheduledTaskManager.TaskEntry task : failedTasks) {
            taskStore.addTaskIfNotExist(task.getName(), task.getState());
            taskManager.removeTaskFromAdditionFailedTaskList(task);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Successfully added the failed task [" + task + "]");
            }
        }
    }

    /**
     * Schedules all tasks assigned to this node.
     *
     * @param state - The state of the tasks which need to be scheduled.
     * @throws TaskCoordinationException - When something goes wrong while retrieving all the assigned tasks.
     */
    private void scheduleAssignedTasks(CoordinatedTask.States state) throws TaskCoordinationException
            , InterruptedException {

        LOG.debug("Retrieving tasks assigned to this node and to be scheduled.");
        List<String> tasksOfThisNode = taskStore.retrieveTaskNames(localNodeId, state);
        if (tasksOfThisNode.isEmpty()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("No tasks assigned to this node to be scheduled in state " + state);
            }
            return;
        }
        List<String> deployedCoordinatedTasks = taskManager.getAllCoordinatedTasksDeployed();
        List<String> erroredTasks = new ArrayList<>();
        for (String taskName : tasksOfThisNode) {
            if (deployedCoordinatedTasks.contains(taskName)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Submitting retrieved task [" + taskName + "] to the task manager.");
                }
                try {
                    checkInterrupted();
                    taskManager.scheduleCoordinatedTask(taskName);
                } catch (TaskException ex) {
                    if (!TaskException.Code.DATABASE_ERROR.equals(ex.getCode())) {
                        erroredTasks.add(taskName);
                    }
                    LOG.error("Exception occurred while scheduling coordinated task : " + taskName, ex);
                }
            } else {
                LOG.info("The task [" + taskName + "] retrieved to be scheduled is not a deployed task "
                                 + "in this node or an invalid entry, hence ignoring it.");
            }
        }
        taskStore.updateTaskState(erroredTasks, CoordinatedTask.States.NONE);
    }

    /**
     * Resolves the un assigned tasks and update the task store.
     * Synchronized since this will be triggered in leader periodically and upon member addition.
     *
     * @throws TaskCoordinationException when something goes wrong connecting to the store
     */
    public synchronized void resolveUnassignedNotCompletedTasksAndUpdateStore() throws TaskCoordinationException {

        List<String> unAssignedTasks = taskStore.retrieveAllUnAssignedAndIncompleteTasks();
        if (unAssignedTasks.isEmpty()) {
            LOG.debug("No un assigned tasks found.");
            return;
        }
        Map<String, String> tasksToBeUpdated = new HashMap<>();
        unAssignedTasks.forEach(taskName -> {
            String destinedNode = taskLocationResolver.getTaskNodeLocation(clusterCommunicator, taskName);
            if (destinedNode != null) { // can't resolve all of the time
                tasksToBeUpdated.put(taskName, destinedNode);
            }
        });
        taskStore.updateAssignmentAndState(tasksToBeUpdated);
    }

    private void notifyOnPause(List<String> pausedTasks) {
        Set<String> completedProcessors = new HashSet();
        Set<String> completedInboundEndpoints = new HashSet();
        SynapseEnvironment synapseEnvironment = MicroIntegratorBaseUtils.getSynapseEnvironment();
        TaskDescriptionRepository taskRepo = synapseEnvironment.getTaskManager().getTaskDescriptionRepository();
        pausedTasks.forEach(task -> {
            if (MiscellaneousUtil.isTaskOfMessageProcessor(task)) {
                String messageProcessorName = MiscellaneousUtil.getMessageProcessorName(task);
                if (!completedProcessors.contains(messageProcessorName)) {
                    MessageProcessor messageProcessor = synapseEnvironment.getSynapseConfiguration()
                            .getMessageProcessors().get(messageProcessorName);
                    if (messageProcessor != null) {
                        messageProcessor.cleanUpDeactivatedProcessors();
                    }
                    completedProcessors.add(messageProcessorName);
                }
            } else {
                TaskDescription taskDescription = taskRepo.getTaskDescription(task);
                if (taskDescription.getProperty(TaskUtils.TASK_OWNER_PROPERTY) == TaskUtils.TASK_BELONGS_TO_INBOUND_ENDPOINT) {
                    String inboundEndpointName = (String) taskDescription.getProperty(TaskUtils.TASK_OWNER_NAME);

                    if (!completedInboundEndpoints.contains(inboundEndpointName)) {
                        InboundEndpoint inboundEndpoint = synapseEnvironment.getSynapseConfiguration()
                                .getInboundEndpoint(inboundEndpointName);
                        if (inboundEndpoint != null) {
                            inboundEndpoint.updateInboundEndpointState(true);
                        }
                        completedInboundEndpoints.add(inboundEndpointName);
                    }
                }
            }
        });
    }
}
