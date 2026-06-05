/*
 * Copyright (c) 2025, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.micro.integrator.dataservices.core.analytics;

import org.apache.synapse.config.SynapsePropertiesLoader;

/**
 * Reads the same Synapse analytics configuration that the existing
 * {@code ElasticStatisticsPublisher} reads, by delegating to
 * {@link SynapsePropertiesLoader} — the canonical loader for
 * {@code synapse.properties}.
 *
 * <p>Deployment-toml renders {@code [analytics]} entries into
 * {@code synapse.properties}, so this helper sees the same values the
 * production publisher sees. Restart MI to pick up config changes (same
 * constraint as the Synapse-side publisher).
 */
final class AnalyticsConfig {

    static final String DEFAULT_ANALYTICS_PREFIX = "SYNAPSE_ANALYTICS_DATA";

    private AnalyticsConfig() {}

    static boolean isAnalyticsEnabled() {
        return SynapsePropertiesLoader.getBooleanProperty("analytics.enabled", false);
    }

    static boolean isDataServiceAnalyticsEnabled() {
        return SynapsePropertiesLoader.getBooleanProperty(
                "analytics.data_service_analytics.enabled", true);
    }

    static String getAnalyticsPrefix() {
        return SynapsePropertiesLoader.getPropertyValue(
                "analytics.prefix", DEFAULT_ANALYTICS_PREFIX);
    }

    static String getPublisherId(String fallback) {
        return SynapsePropertiesLoader.getPropertyValue("analytics.id", fallback);
    }
}
