package com.example;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import groovy.json.JsonSlurper;
import groovy.lang.Binding;
import groovy.lang.GroovyShell;
import groovy.lang.PropertyValue;
import groovy.lang.Script;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.codehaus.groovy.runtime.InvokerHelper;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class NifiGroovyTestScript {

    @Mock
    private ProcessContext context;

    @Mock
    private ProcessSession session;

    @Mock
    private ComponentLog log;

    private Script script;

    @Mock
    private FlowFile mockFlowFile;

    /**
     * @throws Exception
     */
    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);

        // Inside your test setup
        PropertyValue mockedPropertyValue = mock(PropertyValue.class);
        when(mockedPropertyValue.getValue()).thenReturn("8080");
        // Mock the behavior for reading the content of the flow file
        doAnswer(invocation -> {
            InputStreamCallback callback = invocation.getArgument(1, InputStreamCallback.class);
            try (ByteArrayInputStream bais = new ByteArrayInputStream(
                    "{\"messageNumber\":\"300\",\"messageType\":\"MISSION_STATUS\",\"missionId\":\"12501\",\"missionType\":\"Exchange\",\"taskStatus\":[{\"taskId\":\"23001\",\"taskType\":\"Pick\",\"deviceId\":\"Robot05\",\"locationId\":\"2A104\",\"loadId\":\"9000075100\",\"status\":\"SUCCESS\",\"statusDescription\":\"Sent from Tompkins\"}],\"messageTimestamp\":\"2021-08-13T00:51:03.592\"}"
                            .getBytes())) {
                callback.process(bais);
            }
            return null; // For void methods, simply return null
        }).when(session).read(eq(mockFlowFile), any(InputStreamCallback.class));

        // when(context.getProperty(any())).thenReturn(mockedPropertyValue);

        // Initialize Groovy script
        GroovyShell groovyShell = new GroovyShell();
        script = groovyShell.parse(new File("/Users/michaelritchson/Desktop/Tompkins_dev/NIFI_AEGIS_JUNIT_Groovy/AEGIS_TCP_GOLD.groovy"));

        // Set script bindings
        Binding binding = new Binding();
        binding.setVariable("context", context);
        binding.setVariable("session", session);
        binding.setVariable("log", log);
        script.setBinding(binding);
    }

    @Test
    public void testSendFailureResponse() throws Exception {
        // Prepare the data
        // Create the socket and its mock OutputStream
        Socket mockSocket = mock(Socket.class);
        OutputStream mockOutputStream = mock(OutputStream.class);
        when(mockSocket.getOutputStream()).thenReturn(mockOutputStream);

        // Run the method
        Map<String, Object> jsonResponse = null;
        String receivedMessage = "{\n" + //
                "\"messageNumber\":\"200\",\n" + //
                "\"messageType\":\"MISSION\",\n" + //
                "\"missionId\":\"1250100004\",\n" + //
                "\"missionType\":\"Exchange\",\n" + //
                "\"tasks\":[\n" + //
                "{\u201CtaskId\u201D:\u201D23001\u201D,\"taskType\":\"Pick\",\"locationId\":\"2A104\",\u201DloadID\u201D:\u201D9000075100\u201D,\"destinationId\":\u201DTIPPER\u201D},\n"
                + //
                "{\u201CtaskId\u201D:\u201D23002\u201D,\"taskType\":\"Put\",\"locationId\":\"2A104\"}\n" + //
                "{\u201CtaskId\u201D:\u201D23003\u201D,\"taskType\":\"Transfer\",\"loadID\u201D:\u201D9000075100\u201D}\n"
                + //
                "],\n" + //
                "\" messageTimestamp\u201D:\"2021-08-13T00:45:04.592\"}";
        String errorMessage = "Invalid JSON Format: expecting '}' or ',' but got current char with an int value of 34\n"
                + //
                "The current character read is  with an int value of 34\n" + //
                "expecting '}' or ','";

        try {
            InvokerHelper.invokeMethod(script, "sendFailureResponse",
                    new Object[] { mockSocket, receivedMessage, errorMessage });
        } catch (Exception e) {
            e.printStackTrace();
            fail("Exception occurred: " + e.getMessage());
        }

        // Verify that the 'write' method was called on the mock OutputStream
        ArgumentCaptor<byte[]> argumentCaptor = ArgumentCaptor.forClass(byte[].class);
        // verify(mockOutputStream).write(any());
        verify(mockOutputStream).write(argumentCaptor.capture());
        // Convert captured argument to a string
        byte[] responseBytes = argumentCaptor.getValue();
        String response = new String(responseBytes);

        // Perform assertions
        if (response != null && !response.trim().isEmpty()) {
            jsonResponse = (Map<String, Object>) new JsonSlurper().parseText(response);
            assertEquals("failure", jsonResponse.get("status"));
            assertEquals("1", jsonResponse.get("statusCode"));
            assertEquals("2023-09-08T04:26:50.558Z", jsonResponse.get("messageTimestamp"));
            assertEquals("200", jsonResponse.get("messageNumber"));
            assertTrue(jsonResponse.get("statusDesc").toString().contains("Invalid JSON Format: expecting '}' or ','"));
        } else {
            fail("No response was written to the output stream");
        }
    }

    @Test
    public void testSendFlowFileContentToQueue() throws Exception {
        // Mock the behavior for reading the content of the flow file
        doAnswer(invocation -> {
            InputStreamCallback callback = invocation.getArgument(1, InputStreamCallback.class);
            try (ByteArrayInputStream bais = new ByteArrayInputStream(
                    "{\"messageNumber\":\"300\",\"messageType\":\"MISSION_STATUS\",\"missionId\":\"12501\",\"missionType\":\"Exchange\",\"taskStatus\":[{\"taskId\":\"23001\",\"taskType\":\"Pick\",\"deviceId\":\"Robot05\",\"locationId\":\"2A104\",\"loadId\":\"9000075100\",\"status\":\"SUCCESS\",\"statusDescription\":\"Sent from Tompkins\"}],\"messageTimestamp\":\"2021-08-13T00:51:03.592\"}"
                            .getBytes())) {
                callback.process(bais);
            }
            return null; // For void methods, simply return null
        }).when(session).read(eq(mockFlowFile), any(InputStreamCallback.class));

    }

    // TODO: Add more tests as needed
    @Test
    public void testSendKeepAlive() throws Exception {
        // Mock a Socket object and OutputStream
        Socket mockSocket = mock(Socket.class);
        OutputStream mockOutputStream = mock(OutputStream.class);
        when(mockSocket.getOutputStream()).thenReturn(mockOutputStream);

        // Run the method
        try {
            InvokerHelper.invokeMethod(script, "sendKeepAlive",
                    new Object[] { mockSocket});
        } catch (Exception e) {
            e.printStackTrace();
            fail("Exception occurred: " + e.getMessage());
        }

        // Verify that the 'write' method was called on the mock OutputStream
        ArgumentCaptor<byte[]> argumentCaptor = ArgumentCaptor.forClass(byte[].class);
        verify(mockOutputStream).write(argumentCaptor.capture());

        // Convert captured argument to a string
        byte[] sentBytes = argumentCaptor.getValue();
        String keepAliveMessage = new String(sentBytes, StandardCharsets.UTF_8);

        // Perform assertions
        assertTrue(keepAliveMessage.contains("\"messageType\": \"KEEPALIVE\""));
    }

}
