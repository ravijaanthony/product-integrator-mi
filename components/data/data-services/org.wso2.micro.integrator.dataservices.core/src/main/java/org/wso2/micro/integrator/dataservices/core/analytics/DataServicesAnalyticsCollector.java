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

import org.apache.axiom.om.OMElement;
import org.apache.axiom.soap.SOAPEnvelope;
import org.apache.axis2.context.MessageContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.micro.integrator.dataservices.common.DBConstants;
import org.wso2.micro.integrator.dataservices.core.DataServiceFault;

import java.util.Iterator;
import java.util.Map;

import static org.wso2.micro.integrator.dataservices.core.analytics.DataServicesAnalyticsConstants.*;

/**
 * Public API called by every Data Service entry/exit point.
 *
 * Contract (mirrors the existing DataServicesTracingCollector):
 *   - reportEntryEvent(msgCtx)           on entry
 *   - closeEntryEvent(msgCtx, result)    on success
 *   - closeEntryEventWithSubType(...)    on success with a non-default sub-type
 *   - closeFlowForcefully(msgCtx, e)     on failure
 *
 * Idempotent: only the FIRST close-style call per message context publishes,
 * gated by the {@code DS_ANALYTICS_START_TIME} property (set on entry, cleared
 * on publish). Chained invocations on the same axis2 MessageContext each get to
 * emit because every entry sets a fresh start time.
 */
public class DataServicesAnalyticsCollector {

    private static final Log log = LogFactory.getLog(DataServicesAnalyticsCollector.class);

    private DataServicesAnalyticsCollector() {}

    public static void reportEntryEvent(MessageContext msgCtx) {
        if (!isAnalyticsEnabled() || msgCtx == null) return;
        try {
            msgCtx.setProperty(DS_ANALYTICS_START_TIME, System.currentTimeMillis());
        } catch (Exception e) {
            log.warn("Failed to record DS analytics entry event: " + e.getMessage());
        }
    }

    public static void closeEntryEvent(MessageContext msgCtx, OMElement result) {
        DataServiceFault syntheticFault = extractFaultFromResult(result);
        publish(msgCtx, syntheticFault, null);
    }

    public static void closeFlowForcefully(MessageContext msgCtx, Exception e) {
        publish(msgCtx, e, null);
    }

    /**
     * Failure-path equivalent of {@link #closeEntryEventWithSubType(MessageContext, String)}.
     * Use from OData and DSS Call Mediator catch blocks so the resulting analytics
     * event carries the correct {@code subType} (otherwise failures default to
     * {@code DATA_SERVICE}, undercounting ODATA / DSS_CALL_MEDIATOR failure rates).
     */
    public static void closeFlowForcefullyWithSubType(MessageContext msgCtx, Exception e, String subType) {
        publish(msgCtx, e, subType);
    }

    public static void closeEntryEventWithSubType(MessageContext msgCtx, String subType) {
        publish(msgCtx, null, subType);
    }

    /**
     * Sub-type-aware overload that also inspects the {@code result} OMElement
     * for fault content. Use from the OData handler and DSS Call Mediator
     * happy paths so fault responses don't slip through as successes.
     */
    public static void closeEntryEventWithSubType(MessageContext msgCtx, OMElement result, String subType) {
        DataServiceFault syntheticFault = extractFaultFromResult(result);
        publish(msgCtx, syntheticFault, subType);
    }

    public static DataServiceAnalyticsEvent buildEvent(MessageContext msgCtx, Exception error) {
        DataServiceAnalyticsEvent event = new DataServiceAnalyticsEvent();
        event.setSubType(SUB_TYPE_DATA_SERVICE);
        event.setTimestamp(System.currentTimeMillis());

        Long start = (Long) msgCtx.getProperty(DS_ANALYTICS_START_TIME);
        event.setLatency(start != null ? System.currentTimeMillis() - start : 0L);

        event.setServiceName(resolveServiceName(msgCtx));
        event.setOperationName(resolveOperationName(msgCtx));
        event.setMessageId(msgCtx.getMessageID());
        event.setCorrelationId(extractCorrelationId(msgCtx));
        event.setRemoteHost(extractRemoteHost(msgCtx));
        event.setHttpMethod((String) msgCtx.getProperty(HTTP_METHOD_PROPERTY));
        event.setHttpUrl(resolveHttpUrl(msgCtx));

        if (error == null) {
            event.setStatus(STATUS_SUCCESS);
        } else {
            event.setStatus(STATUS_FAILURE);
            if (error instanceof SyntheticDataServiceFault) {
                SyntheticDataServiceFault fault = (SyntheticDataServiceFault) error;
                event.setErrorCode(fault.getCode());
                // Use getDsFaultMessage() (raw message), NOT getMessage() — the latter
                // returns the formatted multi-line "DS Fault Message: ...\nDS Code: ..."
                // string, which would clutter the faultResponse field in Kibana.
                event.setErrorMessage(fault.getDsFaultMessage());
                event.setFaultDetail(fault.getDetail());
            } else if (error instanceof DataServiceFault) {
                DataServiceFault fault = (DataServiceFault) error;
                event.setErrorCode(fault.getCode());
                event.setErrorMessage(fault.getDsFaultMessage());
            } else {
                event.setErrorCode(DBConstants.FaultCodes.UNKNOWN_ERROR);
                event.setErrorMessage(error.getMessage());
            }
        }
        return event;
    }

    private static void publish(MessageContext msgCtx, Exception error, String subType) {
        if (!isAnalyticsEnabled() || msgCtx == null) return;
        try {
            // No entry was opened on this MessageContext (analytics disabled at
            // entry, or already closed by a previous publish on the same flow):
            // skip without emitting a duplicate.
            if (msgCtx.getProperty(DS_ANALYTICS_START_TIME) == null) {
                return;
            }

            DataServiceAnalyticsEvent event = buildEvent(msgCtx, error);
            if (subType != null) {
                event.setSubType(subType);
            }
            DataServicesAnalyticsPublisher.publish(event);
            msgCtx.removeProperty(DS_ANALYTICS_START_TIME);
        } catch (Exception e) {
            log.warn("Failed to publish DS analytics event: " + e.getMessage(), e);
        }
    }

    /**
     * Service-name resolution chain. The explicit override is needed for OData,
     * where the axis service is Synapse's internal {@code __SynapseService} —
     * the real DS name lives in the URL.
     */
    private static String resolveServiceName(MessageContext msgCtx) {
        String override = stringProperty(msgCtx, DS_ANALYTICS_SERVICE_NAME_OVERRIDE);
        if (override != null) return override;
        if (msgCtx.getAxisService() != null) {
            return msgCtx.getAxisService().getName();
        }
        return null;
    }

    /**
     * Operation-name resolution chain.
     * <ol>
     *   <li>Explicit override property (set by OData handler).</li>
     *   <li>{@code axisOperationName} property — set by {@code DataServiceCallMediator}
     *       to the real DS operation.</li>
     *   <li>SOAP body's first child local name — works for direct DS dispatch
     *       (DBInOut / DBInOnly) where the request envelope's body root IS the
     *       operation name (e.g. {@code <_poststudent>...</_poststudent>}).</li>
     *   <li>SOAPAction header.</li>
     *   <li>AxisOperation name — but only if it isn't Synapse's catch-all
     *       {@code "mediate"} (which would always win otherwise inside the
     *       OData / DSS Call Mediator paths and produce useless data).</li>
     * </ol>
     */
    private static String resolveOperationName(MessageContext msgCtx) {
        String override = stringProperty(msgCtx, DS_ANALYTICS_OPERATION_OVERRIDE);
        if (override != null) return override;

        String mediatorOp = stringProperty(msgCtx, MEDIATOR_AXIS_OPERATION_NAME);
        if (mediatorOp != null) return mediatorOp;

        String bodyRoot = firstBodyElementName(msgCtx);
        if (bodyRoot != null) return bodyRoot;

        String soapAction = msgCtx.getSoapAction();
        if (soapAction != null && !soapAction.isEmpty()) return soapAction;

        if (msgCtx.getAxisOperation() != null && msgCtx.getAxisOperation().getName() != null) {
            String localPart = msgCtx.getAxisOperation().getName().getLocalPart();
            if (localPart != null && !localPart.isEmpty() && !SYNAPSE_MEDIATE_OPERATION.equals(localPart)) {
                return localPart;
            }
        }
        return null;
    }

    /**
     * HTTP URL resolution chain. {@code msgCtx.getTo()} is null on the OData
     * path (the handler runs before normal Axis2 dispatch builds the WSA To),
     * so we fall back to the {@code TransportInURL} property which is always
     * present for HTTP inbound requests.
     */
    private static String resolveHttpUrl(MessageContext msgCtx) {
        if (msgCtx.getTo() != null && msgCtx.getTo().getAddress() != null) {
            return msgCtx.getTo().getAddress();
        }
        return stringProperty(msgCtx, TRANSPORT_IN_URL_PROPERTY);
    }

    /**
     * Inspects a DS dispatch result for an embedded Fault element. Used when the
     * underlying {@code DataServiceProcessor.dispatch(...)} returns a fault
     * payload instead of throwing — common for validation errors. Returns
     * {@code null} if the result is a normal payload.
     *
     * <p>Captures the {@code <detail>} child element in addition to
     * {@code faultcode} and {@code faultstring}: detail text goes into
     * {@link SyntheticDataServiceFault#getDetail()} verbatim if present, or
     * the first child element is serialized to XML if {@code <detail>} wraps
     * structured content.
     */
    static DataServiceFault extractFaultFromResult(OMElement result) {
        if (result == null) return null;
        String localName = result.getLocalName();
        if (localName == null) return null;
        // Match SOAP-style "Fault" and DSS-style "DataServiceFault"
        if (!"Fault".equalsIgnoreCase(localName) && !localName.endsWith("Fault")) {
            return null;
        }
        String code = null;
        String message = null;
        String detail = null;
        Iterator<?> children = result.getChildElements();
        while (children.hasNext()) {
            Object child = children.next();
            if (!(child instanceof OMElement)) continue;
            OMElement el = (OMElement) child;
            String n = el.getLocalName();
            if (n == null) continue;
            if ("faultcode".equalsIgnoreCase(n) && code == null) {
                code = el.getText();
            } else if ("faultstring".equalsIgnoreCase(n) && message == null) {
                message = el.getText();
            } else if ("detail".equalsIgnoreCase(n) && detail == null) {
                detail = extractDetail(el);
            }
        }
        if (code == null) code = "DS_FAULT";
        if (message == null) message = "Data service returned a fault response";
        return new SyntheticDataServiceFault(code, message, detail);
    }

    /**
     * If {@code <detail>} wraps a child element (SOAP-style), serialize that
     * element to XML so dashboards keep the structured info. Otherwise return
     * the trimmed text content. Returns {@code null} when empty.
     */
    private static String extractDetail(OMElement detailEl) {
        OMElement firstChild = null;
        Iterator<?> kids = detailEl.getChildren();
        while (kids.hasNext()) {
            Object kid = kids.next();
            if (kid instanceof OMElement) {
                firstChild = (OMElement) kid;
                break;
            }
        }
        if (firstChild != null) {
            try {
                return firstChild.toString();
            } catch (Exception e) {
                // Fall through to text fallback.
            }
        }
        String text = detailEl.getText();
        if (text == null) return null;
        String trimmed = text.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private static String stringProperty(MessageContext msgCtx, String key) {
        Object v = msgCtx.getProperty(key);
        if (v == null) return null;
        String s = v.toString();
        return s.isEmpty() ? null : s;
    }

    private static String firstBodyElementName(MessageContext msgCtx) {
        try {
            SOAPEnvelope env = msgCtx.getEnvelope();
            if (env == null || env.getBody() == null) return null;
            OMElement first = env.getBody().getFirstElement();
            if (first == null) return null;
            String n = first.getLocalName();
            return (n == null || n.isEmpty()) ? null : n;
        } catch (Exception e) {
            return null;
        }
    }

    @SuppressWarnings("rawtypes")
    private static String extractCorrelationId(MessageContext msgCtx) {
        Object headersObj = msgCtx.getProperty(TRANSPORT_HEADERS_PROPERTY);
        if (headersObj instanceof Map) {
            Object cid = ((Map) headersObj).get(CORRELATION_ID_PROPERTY);
            if (cid != null) return cid.toString();
        }
        Object prop = msgCtx.getProperty(CORRELATION_ID_PROPERTY);
        return prop != null ? prop.toString() : null;
    }

    @SuppressWarnings("rawtypes")
    private static String extractRemoteHost(MessageContext msgCtx) {
        Object remoteAddr = msgCtx.getProperty(REMOTE_ADDR_PROPERTY);
        if (remoteAddr != null) return remoteAddr.toString();
        Object headersObj = msgCtx.getProperty(TRANSPORT_HEADERS_PROPERTY);
        if (headersObj instanceof Map) {
            Map headers = (Map) headersObj;
            Object xff = headers.get("X-Forwarded-For");
            if (xff != null) return xff.toString();
        }
        // Deliberately no "Host" header fallback: Host identifies the
        // server/vhost the client called, not the client itself, so using
        // it as remoteHost skews top-clients analytics toward the server's
        // own hostname.
        return null;
    }

    /**
     * Two-layer enable check: global {@code analytics.enabled} AND the
     * Data Services-specific {@code analytics.data_service_analytics.enabled}
     * toggle. Both must be on for the collector to do anything; flipping
     * either to false disables DS analytics emission without affecting the
     * Synapse-side publishers.
     */
    private static boolean isAnalyticsEnabled() {
        return AnalyticsConfig.isAnalyticsEnabled()
                && AnalyticsConfig.isDataServiceAnalyticsEnabled();
    }

    /**
     * In-memory-only {@link DataServiceFault} extension used to carry the
     * captured {@code <detail>} payload from an extractFaultFromResult-derived
     * synthetic fault into the analytics event. Never thrown or surfaced to
     * callers — package-private to keep it an analytics-internal carrier.
     */
    private static final class SyntheticDataServiceFault extends DataServiceFault {

        private static final long serialVersionUID = 1L;

        private final String detail;

        SyntheticDataServiceFault(String code, String message, String detail) {
            super(code, message);
            this.detail = detail;
        }

        String getDetail() {
            return detail;
        }
    }
}
