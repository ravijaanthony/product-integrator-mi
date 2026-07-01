/*
 * Copyright (c) 2026, WSO2 LLC. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.micro.integrator.dataservices.core.analytics;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.ServerConfigurationInformation;
import org.apache.synapse.core.SynapseEnvironment;
import org.json.JSONObject;
import org.wso2.micro.integrator.core.util.MicroIntegratorBaseUtils;

/**
 * Computes the {@code serverInfo} envelope block ({@code hostname},
 * {@code serverName}, {@code ipAddress}, {@code id}) using the same field
 * values the Synapse-side {@code ElasticDataSchema} emits for API/Proxy events.
 *
 * <p>Sourced from {@link ServerConfigurationInformation} — the canonical
 * Synapse-side record of the server's identity (populated at startup from
 * carbon.xml + axis2.xml). This matches exactly what
 * {@code ElasticDataSchema.init()} uses for the production publisher, so DS
 * events carry the same identity tuple as API/Proxy events.
 *
 * <p>If Synapse hasn't initialized yet (very early startup), the resolution
 * returns {@code null} fields safely and the call retries on the next event.
 */
final class ServerInfoProvider {

    private static final Log log = LogFactory.getLog(ServerInfoProvider.class);

    private static volatile Snapshot cachedSnapshot;

    private ServerInfoProvider() {}

    static JSONObject getServerInfo() {
        Snapshot snap = resolveSnapshot();
        JSONObject info = new JSONObject();
        if (snap != null) {
            putIfNotNull(info, "hostname", snap.hostname);
            putIfNotNull(info, "serverName", snap.serverName);
            putIfNotNull(info, "ipAddress", snap.ipAddress);
            putIfNotNull(info, "id", snap.publisherId);
        }
        return info;
    }

    private static Snapshot resolveSnapshot() {
        Snapshot snap = cachedSnapshot;
        if (snap != null) {
            return snap;
        }
        synchronized (ServerInfoProvider.class) {
            if (cachedSnapshot != null) {
                return cachedSnapshot;
            }
            ServerConfigurationInformation info = loadServerInfo();
            if (info == null) {
                // Synapse isn't initialized yet — return a transient null so
                // the next call retries. Don't cache, or we'd be stuck with
                // empty serverInfo for the JVM lifetime.
                return null;
            }
            String hostname = info.getHostName();
            String serverName = info.getServerName();
            String ipAddress = info.getIpAddress();
            String publisherId = AnalyticsConfig.getPublisherId(hostname);
            Snapshot fresh = new Snapshot(hostname, serverName, ipAddress, publisherId);
            cachedSnapshot = fresh;
            return fresh;
        }
    }

    private static ServerConfigurationInformation loadServerInfo() {
        try {
            SynapseEnvironment env = MicroIntegratorBaseUtils.getSynapseEnvironment();
            if (env == null || env.getServerContextInformation() == null) {
                return null;
            }
            return env.getServerContextInformation().getServerConfigurationInformation();
        } catch (Exception e) {
            if (log.isDebugEnabled()) {
                log.debug("Synapse environment not yet available for DS analytics serverInfo: "
                        + e.getMessage());
            }
            return null;
        }
    }

    private static void putIfNotNull(JSONObject obj, String key, String value) {
        if (value != null) {
            obj.put(key, value);
        }
    }

    private static final class Snapshot {
        final String hostname;
        final String serverName;
        final String ipAddress;
        final String publisherId;

        Snapshot(String hostname, String serverName, String ipAddress, String publisherId) {
            this.hostname = hostname;
            this.serverName = serverName;
            this.ipAddress = ipAddress;
            this.publisherId = publisherId;
        }
    }
}
