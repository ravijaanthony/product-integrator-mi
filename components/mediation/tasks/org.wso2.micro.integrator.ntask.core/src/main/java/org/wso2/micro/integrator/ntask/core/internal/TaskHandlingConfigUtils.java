/*
 * Copyright (c) 2024, WSO2 LLC. (http://www.wso2.org) All Rights Reserved.
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
package org.wso2.micro.integrator.ntask.core.internal;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.config.mapper.ConfigParser;

import java.util.Map;

/**
 * Utility methods for task handling configuration flags loaded from deployment.toml.
 */
public final class TaskHandlingConfigUtils {

    private static final Log LOG = LogFactory.getLog(TaskHandlingConfigUtils.class);
    private static final String TASK_HANDLING_CONFIG = "task_handling";
    public static final String TASK_DELETE_BARRIER_ENABLED_CONFIG =
            TASK_HANDLING_CONFIG + ".enable_task_delete_barrier";
    public static final String TASK_MONITORING_ENABLED_CONFIG =
            TASK_HANDLING_CONFIG + ".enable_task_monitoring";

    // Freshness decides how long an observation row means "this task is still running here". It is capped
    // below heartbeat failover so a dead node ages out before reassignment, but stays above two scheduler
    // cycles so a live node that missed a single write is not dropped. The floor is a constant because the
    // scheduler interval (task_handling.resolving_period) is fixed at its 2s default in this deployment;
    // revisit FRESHNESS_WINDOW_FLOOR_MS if resolving_period is ever raised.
    private static final long FRESHNESS_WINDOW_CEILING_MS = 8000L;  // default cap; preserves prior behavior
    private static final long FRESHNESS_WINDOW_FLOOR_MS = 4000L;    // = 2 x the fixed 2s scheduler cycle
    private static final double FRESHNESS_HEARTBEAT_FACTOR = 0.75d; // stay strictly below the reassignment point

    private TaskHandlingConfigUtils() {

    }

    /**
     * Returns whether clustered task delete barrier flow is enabled.
     * Defaults to true unless explicitly disabled via deployment.toml.
     *
     * @return true when barrier flow is enabled (default), false only when explicitly disabled in deployment.toml
     */
    public static boolean isTaskDeleteBarrierEnabled() {
        try {
            Map<String, Object> configs = ConfigParser.getParsedConfigs();
            if (configs == null) {
                return true;
            }
            Object value = configs.get(TASK_DELETE_BARRIER_ENABLED_CONFIG);
            if (value == null) {
                return true;
            }
            if (value instanceof Boolean) {
                return (Boolean) value;
            }
            return Boolean.parseBoolean(value.toString());
        } catch (Throwable throwable) {
            LOG.warn("Unable to read deployment.toml config [" + TASK_DELETE_BARRIER_ENABLED_CONFIG
                    + "]. Defaulting to enabled barrier flow.");
            return true;
        }
    }

    /**
     * Returns whether coordinated task monitoring is enabled. The feature is disabled by default so
     * observation writes, duplicate detection, and the management API remain inert unless deployment.toml
     * explicitly enables them.
     *
     * @return true when task monitoring is enabled in deployment.toml
     */
    public static boolean isTaskMonitoringEnabled() {
        try {
            Map<String, Object> configs = ConfigParser.getParsedConfigs();
            if (configs == null) {
                return false;
            }
            Object value = configs.get(TASK_MONITORING_ENABLED_CONFIG);
            if (value == null) {
                return false;
            }
            if (value instanceof Boolean) {
                return (Boolean) value;
            }
            return Boolean.parseBoolean(value.toString());
        } catch (Throwable throwable) {
            LOG.warn("Unable to read deployment.toml config [" + TASK_MONITORING_ENABLED_CONFIG
                    + "]. Defaulting to disabled task monitoring.");
            return false;
        }
    }

    /**
     * Computes how old an observation can be before it is ignored. The window is kept below heartbeat
     * failover so a dead node disappears before its task is reassigned, and above two scheduler cycles so a
     * live node that missed a single write is not dropped. If the heartbeat interval is too low for both to
     * hold, the floor wins (live nodes stay visible) and the detector may report short transient duplicates.
     *
     * @param heartbeatMaxRetryIntervalMs configured heartbeat retry interval in milliseconds; {@code <= 0}
     *                                    means no coordinator timing is available
     * @return freshness window in milliseconds
     */
    public static long computeDuplicationFreshnessWindowMs(int heartbeatMaxRetryIntervalMs) {
        if (heartbeatMaxRetryIntervalMs <= 0) {
            // Without coordinator timing there is no failover ceiling, so use the default safe window.
            return FRESHNESS_WINDOW_CEILING_MS;
        }
        long derived = (long) (heartbeatMaxRetryIntervalMs * FRESHNESS_HEARTBEAT_FACTOR);
        return Math.max(FRESHNESS_WINDOW_FLOOR_MS, Math.min(FRESHNESS_WINDOW_CEILING_MS, derived));
    }
}
