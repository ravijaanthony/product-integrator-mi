/*
 * Copyright (c) 2026, WSO2 LLC. (http://www.wso2.com) All Rights Reserved.
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
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

package org.wso2.micro.integrator.management.apis;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.MessageContext;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.json.JSONArray;
import org.json.JSONObject;
import org.wso2.carbon.inbound.endpoint.internal.http.api.APIResource;
import org.wso2.micro.integrator.ndatasource.core.CarbonDataSource;
import org.wso2.micro.integrator.ndatasource.core.DataSourceManager;
import org.wso2.micro.integrator.ndatasource.core.DataSourceRepository;
import org.wso2.micro.integrator.ntask.core.impl.standalone.ScheduledTaskManager;
import org.wso2.micro.integrator.ntask.core.internal.DataHolder;
import org.wso2.micro.integrator.ntask.core.internal.TaskHandlingConfigUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.sql.DataSource;

/**
 * Read-only management resource for coordinated-task runtime status. Cluster-wide views read the shared
 * coordination database, where each node publishes its current running-task snapshot. A task reported by
 * two or more nodes is marked as a duplicate.
 *
 * <pre>
 * GET /management/task-status                 -> { "nodes": [ { "nodeId", "tasks":[...] } ] }
 * GET /management/task-status?name=&lt;task&gt;     -> { "taskName", "runningOn":[...], "duplicate", "observedAt" }
 * GET /management/task-status?view=duplicates -> { "healthy", "duplicates":[ { "taskName", "nodes":[...] } ] }
 * GET /management/task-status?scope=local     -> { "scope":"local", "nodeId", "tasks":[...] }  (this node only)
 * </pre>
 *
 * If the coordination database is unavailable, DB-backed views return a degraded response instead of a
 * server error. The {@code scope=local} view is different: it reads this node's in-memory running set and
 * can still answer during a coordination DB outage.
 */
public class CoordinatedTaskStatusResource extends APIResource {

    private static final Log LOG = LogFactory.getLog(CoordinatedTaskStatusResource.class);
    private static final String COORDINATION_DB = "WSO2_COORDINATION_DB";
    private static final String SELECT_FRESH =
            "SELECT NODE_ID, TASK_NAME, OBSERVED_AT FROM RUNNING_TASK_OBSERVATION WHERE OBSERVED_AT >= ?";
    private static final String EPISODE_COLUMNS =
            "TASK_NAME, NODES, DESTINED_NODE, DETECTED_AT, CLEARED_AT, SEVERITY, TASK_KIND FROM TASK_DUPLICATION_EVENT";
    private static final String SELECT_EPISODES = "SELECT " + EPISODE_COLUMNS + " ORDER BY DETECTED_AT DESC";
    private static final String SELECT_EPISODES_BY_TASK =
            "SELECT " + EPISODE_COLUMNS + " WHERE TASK_NAME = ? ORDER BY DETECTED_AT DESC";
    private static final int QUERY_TIMEOUT_SECONDS = 5;

    public CoordinatedTaskStatusResource(String urlTemplate) {
        super(urlTemplate);
    }

    @Override
    public Set<String> getMethods() {
        Set<String> methods = new HashSet<>();
        methods.add(Constants.HTTP_GET);
        return methods;
    }

    @Override
    public boolean invoke(MessageContext messageContext) {

        buildMessage(messageContext);
        org.apache.axis2.context.MessageContext axis2MessageContext =
                ((Axis2MessageContext) messageContext).getAxis2MessageContext();
        // Monitoring is opt-in. When disabled, report that state instead of reading tables no node writes.
        if (!TaskHandlingConfigUtils.isTaskMonitoringEnabled()) {
            JSONObject disabled = new JSONObject();
            disabled.put("taskMonitoringEnabled", false);
            disabled.put("message", "Task monitoring is disabled. Set [task_handling] "
                    + "enable_task_monitoring = true in deployment.toml to enable it.");
            Utils.setJsonPayLoad(axis2MessageContext, disabled);
            axis2MessageContext.removeProperty(Constants.NO_ENTITY_BODY);
            return true;
        }
        // Local scope intentionally avoids the coordination DB and reports only this node's in-memory set.
        if ("local".equalsIgnoreCase(Utils.getQueryParameter(messageContext, "scope"))) {
            Utils.setJsonPayLoad(axis2MessageContext, localInMemory());
            axis2MessageContext.removeProperty(Constants.NO_ENTITY_BODY);
            return true;
        }
        try {
            String name = Utils.getQueryParameter(messageContext, "name");
            String node = Utils.getQueryParameter(messageContext, "node");
            String view = Utils.getQueryParameter(messageContext, "view");
            JSONObject body;
            if ("history".equalsIgnoreCase(view)) {
                body = episodeHistory(name);
            } else {
                List<Observation> fresh = readFreshObservations();
                if (name != null) {
                    body = perTask(fresh, name);
                } else if (node != null) {
                    body = perNode(fresh, parseNodes(node));
                } else if ("duplicates".equalsIgnoreCase(view)) {
                    body = duplicates(fresh);
                } else {
                    body = perNode(fresh, null);
                }
            }
            Utils.setJsonPayLoad(axis2MessageContext, body);
        } catch (Throwable t) {
            // Cluster-wide status is best-effort. If the shared DB cannot be read, return a clear
            // degraded response instead of failing the management API call.
            LOG.warn("Unable to read coordinated task status from " + COORDINATION_DB
                    + "; returning a degraded response.", t);
            JSONObject body = new JSONObject();
            body.put("coordinationDbAvailable", false);
            body.put("message", "Coordination DB unavailable; cluster aggregation not possible right now.");
            Utils.setJsonPayLoad(axis2MessageContext, body);
        }
        axis2MessageContext.removeProperty(Constants.NO_ENTITY_BODY);
        return true;
    }

    /**
     * Builds the default node-grouped response. When a node filter is supplied, requested nodes are kept
     * in the response even if they have no fresh observations.
     */
    private JSONObject perNode(List<Observation> fresh, Set<String> nodeFilter) {
        Map<String, JSONArray> byNode = new LinkedHashMap<>();
        // Preserve explicitly requested node IDs even when they currently have no fresh task rows.
        if (nodeFilter != null) {
            for (String requested : nodeFilter) {
                byNode.put(requested, new JSONArray());
            }
        }
        for (Observation o : fresh) {
            if (nodeFilter != null && !nodeFilter.contains(o.nodeId)) {
                continue;
            }
            byNode.computeIfAbsent(o.nodeId, k -> new JSONArray()).put(o.taskName);
        }
        JSONArray nodes = new JSONArray();
        for (Map.Entry<String, JSONArray> entry : byNode.entrySet()) {
            JSONObject node = new JSONObject();
            node.put("nodeId", entry.getKey());
            node.put("tasks", entry.getValue());
            nodes.put(node);
        }
        JSONObject body = new JSONObject();
        body.put("coordinationDbAvailable", true);
        body.put("nodes", nodes);
        return body;
    }

    /**
     * Parses the comma-separated node query parameter while preserving the caller's requested order.
     */
    private Set<String> parseNodes(String nodeParam) {
        Set<String> nodes = new LinkedHashSet<>();
        for (String value : nodeParam.split(",")) {
            String trimmed = value.trim();
            if (!trimmed.isEmpty()) {
                nodes.add(trimmed);
            }
        }
        return nodes;
    }

    /**
     * Builds the status response for one task, including the nodes that recently reported running it and
     * whether that count makes it a duplicate.
     */
    private JSONObject perTask(List<Observation> fresh, String taskName) {
        List<String> runningOn = new ArrayList<>();
        long observedAt = -1L;
        for (Observation o : fresh) {
            if (o.taskName.equals(taskName)) {
                if (!runningOn.contains(o.nodeId)) {
                    runningOn.add(o.nodeId);
                }
                observedAt = Math.max(observedAt, o.observedAt);
            }
        }
        JSONObject body = new JSONObject();
        body.put("coordinationDbAvailable", true);
        body.put("taskName", taskName);
        body.put("runningOn", new JSONArray(runningOn));
        body.put("duplicate", runningOn.size() >= 2);
        body.put("observedAt", observedAt < 0 ? JSONObject.NULL : Instant.ofEpochMilli(observedAt).toString());
        return body;
    }

    /**
     * Builds the duplicate-only view by grouping fresh observations by task and returning only tasks seen
     * on more than one node.
     */
    private JSONObject duplicates(List<Observation> fresh) {
        Map<String, Set<String>> nodesByTask = new LinkedHashMap<>();
        for (Observation o : fresh) {
            nodesByTask.computeIfAbsent(o.taskName, k -> new HashSet<>()).add(o.nodeId);
        }
        JSONArray dups = new JSONArray();
        for (Map.Entry<String, Set<String>> entry : nodesByTask.entrySet()) {
            if (entry.getValue().size() >= 2) {
                JSONObject dup = new JSONObject();
                dup.put("taskName", entry.getKey());
                dup.put("nodes", new JSONArray(entry.getValue()));
                dups.put(dup);
            }
        }
        JSONObject body = new JSONObject();
        body.put("coordinationDbAvailable", true);
        body.put("healthy", dups.length() == 0);
        body.put("duplicates", dups);
        return body;
    }

    /**
     * Builds the node-local view from this process memory only. This is useful when the coordination DB
     * is unavailable, but it is not a cluster-wide answer; callers must query each node for its local set.
     */
    private JSONObject localInMemory() {
        JSONObject body = new JSONObject();
        body.put("scope", "local");
        body.put("source", "in-memory");
        try {
            DataHolder dataHolder = DataHolder.getInstance();
            String nodeId = dataHolder.getLocalNodeId();
            body.put("nodeId", nodeId == null ? JSONObject.NULL : nodeId);
            body.put("coordinationEnabled", dataHolder.isCoordinationEnabledGlobally());
            JSONArray tasks = new JSONArray();
            ScheduledTaskManager taskManager = dataHolder.getTaskManager();
            if (taskManager != null) {
                for (String task : taskManager.getLocallyRunningCoordinatedTasks()) {
                    tasks.put(task);
                }
            }
            body.put("tasks", tasks);
        } catch (Throwable t) {
            // Local view is a fallback path. Return an explanatory payload if the task subsystem is not
            // available instead of converting the fallback into a server error.
            LOG.warn("Unable to read local in-memory coordinated task set on this node.", t);
            body.put("available", false);
            body.put("message", "Local coordinated task subsystem is not available on this node.");
        }
        return body;
    }

    /**
     * Reads stored duplicate-execution episodes for audit/history views. When a task name is supplied,
     * only episodes for that task are returned.
     */
    private JSONObject episodeHistory(String taskName) throws Exception {
        DataSource dataSource = getCoordinationDataSource();
        JSONArray episodes = new JSONArray();
        try (Connection connection = dataSource.getConnection();
                PreparedStatement ps = connection.prepareStatement(
                        taskName == null ? SELECT_EPISODES : SELECT_EPISODES_BY_TASK)) {
            ps.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
            if (taskName != null) {
                ps.setString(1, taskName);
            }
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    JSONObject episode = new JSONObject();
                    episode.put("taskName", rs.getString("TASK_NAME"));
                    episode.put("nodes", rs.getString("NODES"));
                    episode.put("destinedNode", rs.getString("DESTINED_NODE"));
                    long detectedAt = rs.getLong("DETECTED_AT");
                    episode.put("detectedAt", Instant.ofEpochMilli(detectedAt).toString());
                    long clearedAt = rs.getLong("CLEARED_AT");
                    boolean stillOpen = rs.wasNull();
                    episode.put("clearedAt", stillOpen ? JSONObject.NULL : Instant.ofEpochMilli(clearedAt).toString());
                    episode.put("durationMs", stillOpen ? JSONObject.NULL : (clearedAt - detectedAt));
                    String severity = rs.getString("SEVERITY");
                    episode.put("severity", severity == null ? "OPEN" : severity);
                    episode.put("kind", rs.getString("TASK_KIND"));
                    episode.put("open", stillOpen);
                    episodes.put(episode);
                }
            }
        }
        JSONObject body = new JSONObject();
        body.put("coordinationDbAvailable", true);
        body.put("episodes", episodes);
        return body;
    }

    /**
     * Reads DB-backed observations that are still within the same freshness window used by the leader
     * detector, so management output matches detector behavior.
     */
    private List<Observation> readFreshObservations() throws Exception {
        DataSource dataSource = getCoordinationDataSource();
        // Use the same freshness rule as the leader detector so API output and detector decisions agree.
        long freshnessWindow = TaskHandlingConfigUtils.computeDuplicationFreshnessWindowMs(
                DataHolder.getInstance().getHeartbeatMaxRetryInterval());
        long minObservedAt = System.currentTimeMillis() - freshnessWindow;
        List<Observation> result = new ArrayList<>();
        try (Connection connection = dataSource.getConnection();
                PreparedStatement ps = connection.prepareStatement(SELECT_FRESH)) {
            ps.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
            ps.setLong(1, minObservedAt);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    result.add(new Observation(rs.getString("NODE_ID"), rs.getString("TASK_NAME"),
                            rs.getLong("OBSERVED_AT")));
                }
            }
        }
        return result;
    }

    /**
     * Returns the shared coordination datasource used by the coordinated-task store and observation API.
     */
    private DataSource getCoordinationDataSource() throws Exception {
        DataSourceRepository repository = DataSourceManager.getInstance().getDataSourceRepository();
        CarbonDataSource carbonDataSource = repository.getDataSource(COORDINATION_DB);
        if (carbonDataSource == null || !(carbonDataSource.getDSObject() instanceof DataSource)) {
            throw new IllegalStateException(COORDINATION_DB + " datasource is not available.");
        }
        return (DataSource) carbonDataSource.getDSObject();
    }

    private static final class Observation {
        private final String nodeId;
        private final String taskName;
        private final long observedAt;

        private Observation(String nodeId, String taskName, long observedAt) {
            this.nodeId = nodeId;
            this.taskName = taskName;
            this.observedAt = observedAt;
        }
    }
}
