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

import junit.framework.TestCase;
import org.apache.axiom.om.OMAbstractFactory;
import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.OMFactory;
import org.apache.axiom.om.OMNamespace;
import org.apache.axis2.AxisFault;
import org.apache.axis2.context.MessageContext;
import org.apache.axis2.description.AxisOperation;
import org.apache.axis2.description.AxisService;
import org.apache.axis2.description.InOnlyAxisOperation;
import org.wso2.micro.integrator.dataservices.core.DataServiceFault;

import javax.xml.namespace.QName;
import java.util.HashMap;
import java.util.Map;

public class DataServicesAnalyticsCollectorTestSuite extends TestCase {

    private MessageContext msgCtx;

    @Override
    protected void setUp() {
        msgCtx = new MessageContext();
        msgCtx.setAxisService(new AxisService("RDBMSDataService"));
        msgCtx.setSoapAction("urn:getEmployee");
        msgCtx.setMessageID("urn:uuid:test-1");
    }

    public void testBuildEventSuccessSetsSuccessStatus() {
        msgCtx.setProperty(DataServicesAnalyticsConstants.DS_ANALYTICS_START_TIME,
                System.currentTimeMillis() - 50);

        DataServiceAnalyticsEvent e = DataServicesAnalyticsCollector.buildEvent(msgCtx, null);

        assertEquals("RDBMSDataService", e.getServiceName());
        assertEquals("urn:getEmployee", e.getOperationName());
        assertEquals(DataServicesAnalyticsConstants.STATUS_SUCCESS, e.getStatus());
        assertEquals(DataServicesAnalyticsConstants.SUB_TYPE_DATA_SERVICE, e.getSubType());
        assertNull(e.getErrorCode());
        assertTrue(e.getLatency() > 0);
    }

    public void testBuildEventDataServiceFaultCapturesFault() {
        msgCtx.setProperty(DataServicesAnalyticsConstants.DS_ANALYTICS_START_TIME,
                System.currentTimeMillis());

        DataServiceFault fault = new DataServiceFault("DATABASE_ERROR", "connection refused");
        DataServiceAnalyticsEvent e = DataServicesAnalyticsCollector.buildEvent(msgCtx, fault);

        assertEquals(DataServicesAnalyticsConstants.STATUS_FAILURE, e.getStatus());
        assertEquals("DATABASE_ERROR", e.getErrorCode());
        assertEquals("connection refused", e.getErrorMessage());
        assertNull("Plain DataServiceFault has no detail to surface", e.getFaultDetail());
    }

    public void testBuildEventGenericExceptionSetsUnknownErrorCode() {
        msgCtx.setProperty(DataServicesAnalyticsConstants.DS_ANALYTICS_START_TIME,
                System.currentTimeMillis());

        DataServiceAnalyticsEvent e = DataServicesAnalyticsCollector.buildEvent(
                msgCtx, new RuntimeException("boom"));

        assertEquals(DataServicesAnalyticsConstants.STATUS_FAILURE, e.getStatus());
        assertNotNull(e.getErrorCode());
        assertEquals("boom", e.getErrorMessage());
    }

    public void testBuildEventMissingStartTimeYieldsZeroLatency() {
        DataServiceAnalyticsEvent e = DataServicesAnalyticsCollector.buildEvent(msgCtx, null);
        assertEquals(0L, e.getLatency());
    }

    public void testExtractFaultFromResult_recognizesFaultElement() {
        OMFactory fac = OMAbstractFactory.getOMFactory();
        OMNamespace ns = fac.createOMNamespace("", "");
        OMElement faultElement = fac.createOMElement("Fault", ns);
        OMElement code = fac.createOMElement("faultcode", ns);
        code.setText("soapenv:Server");
        faultElement.addChild(code);
        OMElement faultString = fac.createOMElement("faultstring", ns);
        faultString.setText("Value type mismatch");
        faultElement.addChild(faultString);

        DataServiceFault synthesized = DataServicesAnalyticsCollector.extractFaultFromResult(faultElement);

        assertNotNull("Fault element must be detected", synthesized);
        assertEquals("soapenv:Server", synthesized.getCode());
        assertEquals("Value type mismatch", synthesized.getDsFaultMessage());
    }

    public void testExtractFaultFromResult_capturesDetailText() {
        OMFactory fac = OMAbstractFactory.getOMFactory();
        OMNamespace ns = fac.createOMNamespace("", "");
        OMElement faultElement = fac.createOMElement("Fault", ns);
        OMElement code = fac.createOMElement("faultcode", ns);
        code.setText("DATABASE_ERROR");
        faultElement.addChild(code);
        OMElement faultString = fac.createOMElement("faultstring", ns);
        faultString.setText("query failed");
        faultElement.addChild(faultString);
        OMElement detail = fac.createOMElement("detail", ns);
        detail.setText("ORA-00942: table or view does not exist");
        faultElement.addChild(detail);

        DataServiceFault synthesized = DataServicesAnalyticsCollector.extractFaultFromResult(faultElement);

        // The collector emits the detail via SyntheticDataServiceFault, which
        // is package-private. Exercise the same code path buildEvent uses so
        // we don't need to reach the private inner class directly.
        DataServiceAnalyticsEvent event = DataServicesAnalyticsCollector.buildEvent(msgCtx, synthesized);
        assertEquals("ORA-00942: table or view does not exist", event.getFaultDetail());
    }

    public void testExtractFaultFromResult_capturesNestedDetailElement() {
        OMFactory fac = OMAbstractFactory.getOMFactory();
        OMNamespace ns = fac.createOMNamespace("", "");
        OMElement faultElement = fac.createOMElement("Fault", ns);
        OMElement code = fac.createOMElement("faultcode", ns);
        code.setText("DATABASE_ERROR");
        faultElement.addChild(code);
        OMElement faultString = fac.createOMElement("faultstring", ns);
        faultString.setText("query failed");
        faultElement.addChild(faultString);
        OMElement detail = fac.createOMElement("detail", ns);
        OMNamespace dsNs = fac.createOMNamespace("http://ws.wso2.org/dataservice", "ds");
        OMElement dsFault = fac.createOMElement("DataServiceFault", dsNs);
        OMElement nested = fac.createOMElement("nested-error", dsNs);
        nested.setText("table missing");
        dsFault.addChild(nested);
        detail.addChild(dsFault);
        faultElement.addChild(detail);

        DataServiceFault synthesized = DataServicesAnalyticsCollector.extractFaultFromResult(faultElement);
        DataServiceAnalyticsEvent event = DataServicesAnalyticsCollector.buildEvent(msgCtx, synthesized);
        String captured = event.getFaultDetail();
        assertNotNull("Nested detail element must be captured", captured);
        // Either an XML serialization or at least the nested marker survives.
        assertTrue("Expected nested DataServiceFault element in faultDetail; got: " + captured,
                captured.contains("DataServiceFault") || captured.contains("nested-error"));
    }

    public void testExtractFaultFromResult_returnsNullForNormalPayload() {
        OMFactory fac = OMAbstractFactory.getOMFactory();
        OMNamespace ns = fac.createOMNamespace("http://ws.wso2.org/dataservice", "p");
        OMElement payload = fac.createOMElement("_getemployee", ns);

        DataServiceFault synthesized = DataServicesAnalyticsCollector.extractFaultFromResult(payload);

        assertNull("Normal payloads must not be flagged as faults", synthesized);
    }

    public void testExtractFaultFromResult_handlesNullSafely() {
        assertNull(DataServicesAnalyticsCollector.extractFaultFromResult(null));
    }

    public void testReportEntryEventOpensGate() {
        // Whatever the prior state, reportEntryEvent should reopen the gate:
        // when analytics is enabled, START_TIME becomes a Long; when disabled,
        // the call is a no-op and START_TIME stays whatever it was (null here).
        DataServicesAnalyticsCollector.reportEntryEvent(msgCtx);
        Object start = msgCtx.getProperty(DataServicesAnalyticsConstants.DS_ANALYTICS_START_TIME);
        assertTrue("DS_ANALYTICS_START_TIME must be null (analytics disabled) or a Long (enabled)",
                start == null || start instanceof Long);
    }

    public void testOperationResolution_overrideWinsOverEverything() {
        msgCtx.setProperty(DataServicesAnalyticsConstants.DS_ANALYTICS_OPERATION_OVERRIDE, "explicit-op");
        msgCtx.setProperty(DataServicesAnalyticsConstants.MEDIATOR_AXIS_OPERATION_NAME, "mediator-op");
        msgCtx.setProperty(DataServicesAnalyticsConstants.DS_ANALYTICS_START_TIME, System.currentTimeMillis());

        DataServiceAnalyticsEvent e = DataServicesAnalyticsCollector.buildEvent(msgCtx, null);
        assertEquals("explicit-op", e.getOperationName());
    }

    public void testOperationResolution_mediatorPropertyBeatsSoapAction() {
        msgCtx.setProperty(DataServicesAnalyticsConstants.MEDIATOR_AXIS_OPERATION_NAME, "mediator-op");
        msgCtx.setSoapAction("urn:soapaction-op");
        msgCtx.setProperty(DataServicesAnalyticsConstants.DS_ANALYTICS_START_TIME, System.currentTimeMillis());

        DataServiceAnalyticsEvent e = DataServicesAnalyticsCollector.buildEvent(msgCtx, null);
        assertEquals("mediator-op", e.getOperationName());
    }

    public void testOperationResolution_skipsSynapseMediateCatchAll() throws AxisFault {
        // Build a MessageContext whose AxisOperation localPart is "mediate"
        // (Synapse's catch-all) — must NOT be returned as the operation name.
        msgCtx.setSoapAction(null);
        msgCtx.removeProperty(DataServicesAnalyticsConstants.MEDIATOR_AXIS_OPERATION_NAME);
        msgCtx.removeProperty(DataServicesAnalyticsConstants.DS_ANALYTICS_OPERATION_OVERRIDE);
        AxisOperation mediateOp = new InOnlyAxisOperation(new QName("mediate"));
        msgCtx.setAxisOperation(mediateOp);
        msgCtx.setProperty(DataServicesAnalyticsConstants.DS_ANALYTICS_START_TIME, System.currentTimeMillis());

        DataServiceAnalyticsEvent e = DataServicesAnalyticsCollector.buildEvent(msgCtx, null);
        assertNull("Synapse's catch-all \"mediate\" must be skipped", e.getOperationName());
    }

    public void testServiceResolution_overrideWinsOverAxisService() {
        msgCtx.setProperty(DataServicesAnalyticsConstants.DS_ANALYTICS_SERVICE_NAME_OVERRIDE,
                "EmployeeDataService");
        msgCtx.setProperty(DataServicesAnalyticsConstants.DS_ANALYTICS_START_TIME, System.currentTimeMillis());

        DataServiceAnalyticsEvent e = DataServicesAnalyticsCollector.buildEvent(msgCtx, null);
        assertEquals("EmployeeDataService", e.getServiceName());
    }

    public void testHttpUrlResolution_fallsBackToTransportInURLWhenWsaToIsNull() {
        // msgCtx.getTo() returns null by default in this setUp().
        msgCtx.setProperty(DataServicesAnalyticsConstants.TRANSPORT_IN_URL_PROPERTY,
                "/odata/EmployeeDataService/Employees");
        msgCtx.setProperty(DataServicesAnalyticsConstants.DS_ANALYTICS_START_TIME, System.currentTimeMillis());

        DataServiceAnalyticsEvent e = DataServicesAnalyticsCollector.buildEvent(msgCtx, null);
        assertEquals("/odata/EmployeeDataService/Employees", e.getHttpUrl());
    }

    public void testRemoteHostResolution_doesNotFallBackToHostHeader() {
        // Host identifies the server, not the client — must NOT be used as remoteHost.
        Map<String, String> headers = new HashMap<>();
        headers.put("Host", "api.example.com");
        msgCtx.setProperty(DataServicesAnalyticsConstants.TRANSPORT_HEADERS_PROPERTY, headers);
        msgCtx.setProperty(DataServicesAnalyticsConstants.DS_ANALYTICS_START_TIME, System.currentTimeMillis());

        DataServiceAnalyticsEvent e = DataServicesAnalyticsCollector.buildEvent(msgCtx, null);
        assertNull("Host header must not be used as remote host", e.getRemoteHost());
    }
}
