/* groovylint-disable MethodParameterTypeRequired, NoDef */
import java.net.ServerSocket
import java.net.Socket
import java.io.OutputStream
import java.io.InputStream
import groovy.json.JsonSlurper
import org.apache.nifi.processor.io.InputStreamCallback
import java.util.Random

// Add the stub here for unit tests only
abstract class OutputStreamCallback {
    abstract void process(OutputStream outputStream) throws IOException
}

// Parameters for the TCP server (listener) and remote server (client)
def serverPort = context.getProperty('TCPPort').evaluateAttributeExpressions().getValue() as int
def remoteHost = context.getProperty('ServerHost').evaluateAttributeExpressions().getValue()
def remotePort = context.getProperty('TCPPort').evaluateAttributeExpressions().getValue() as int

// Start the TCP server (listener)
def serverSocket = new ServerSocket(serverPort)


/* groovylint-disable-next-line UnusedMethodParameter */
def sendFailureResponse(socket, receivedMessage, errorMessage) {
    /* groovylint-disable-next-line UnnecessaryGetter */
    try {
        def trimmedErrorMessage = errorMessage.size() > 150 ? errorMessage.substring(0, 150) : errorMessage

        def random = new Random()
        def randomMessageNumber = random.nextInt(1000000000).toString()
        def failureMessage = """
        {
            "messageNumber": "200",
            "status": "failure",
            "statusCode": "1",
            "statusDesc": "Invalid JSON Format: $trimmedErrorMessage",
            "messageTimestamp": "2023-09-08T04:26:50.558Z"
        }
        """

        socket.getOutputStream().write(failureMessage.getBytes('UTF-8'))
        socket.getOutputStream().flush()
        log.info("Sent failure response to client: $failureMessage")

        def flowFile = session.create()
        flowFile = session.write(flowFile, { outputStream ->
            outputStream.write(failureMessage.getBytes('UTF-8'))
        } as OutputStreamCallback)
        flowFile = session.putAttribute(flowFile, 'error-invalid-JSON', trimmedErrorMessage)
        flowFile = session.putAttribute(flowFile, 'received-message-aegis', receivedMessage)
        session.transfer(flowFile, REL_FAILURE)
        session.commit()
        socket.getOutputStream().write(failureMessage.getBytes('UTF-8'))
    } catch (Exception ex) {
        log.error("Error sending failure response: ${ex.message}", ex)
    }
}


def handleClient(socket) {
    try {
        def input = socket.getInputStream()
        def output = socket.getOutputStream()

        while (!Thread.currentThread().isInterrupted()) {
            def buffer = new byte[1024]
            int bytesRead = input.read(buffer)

            if (bytesRead == -1) {
                log.warn('Client disconnected')
                throw new Exception('Client disconnected')
            }

            def receivedMessage = new String(buffer, 0, bytesRead)
            log.debug("Received TCP message from client: $receivedMessage")

            def json
            try {
                def slurper = new JsonSlurper()
                json = slurper.parseText(receivedMessage)
            } catch (Exception ex) {
                log.error("Error parsing JSON: ${ex.message}", ex)
                sendFailureResponse(socket, receivedMessage, ex.message)
                return
            }

            def messageNumber = json.messageNumber ?: 'Unknown'
            def messageType = json.messageType ?: 'Unknown'

            def responseMessage = """
            {
                "messageNumber": "$messageNumber",
                "status": "success",
                "statusCode": "0",
                "statusDesc": "Message processed successfully",
                "messageTimestamp": "${new Date().format("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", TimeZone.getTimeZone('UTC'))}"
            }
            """
            output.write(responseMessage.getBytes('UTF-8'))
            output.flush()
            log.error("Sent response to client: $responseMessage")

            // Send the FlowFile content (if available) to the Hercules client
            sendFlowFileContentToClient(socket)
        }
    } catch (Exception e) {
        log.error("Error while handling client connection: ${e.message}", e)
        log.warn('Waiting 10 seconds before accepting the next connection...')
        Thread.sleep(10000)  // Sleep for 10 seconds
        return
    }
}

def sendKeepAlive(socket) {
    def random = new Random()
    //for (int i = 0; i < times; i++) {
     try {
            def randomMessageNumber = random.nextInt(1000000000).toString()
            def keepAliveMessage = """
            {
                "messageNumber": "$randomMessageNumber",
                "messageType": "KEEPALIVE",
                "messageTimestamp": "2023-09-08T04:26:50.558Z"
            }
            """
            socket.getOutputStream().write(keepAliveMessage.getBytes('UTF-8'))
            socket.getOutputStream().flush()
            log.info('Sent keep-alive message to client')
            
        } catch (Exception e) {
            log.error("Error while sending keep-alive: ${e.message}", e)
       }
    
}

def shutdownHook = new Thread({
    log.info('Shutting down server socket...')
    serverSocket.close()
})
Runtime.getRuntime().addShutdownHook(shutdownHook)

while (true) {
    try {
        def clientSocket = serverSocket.accept()
        def keepAliveThread = new Thread({ sendKeepAlive(clientSocket,1) })
        keepAliveThread.start() // Start keep-alive thread
        handleClient(clientSocket)
    } catch (InterruptedException e) {
        log.info('Script terminated, closing server socket...')
        serverSocket.close() // Close the server socket first
        shutdownHook.join() // Wait for the shutdown hook to complete
        break
    }
}
