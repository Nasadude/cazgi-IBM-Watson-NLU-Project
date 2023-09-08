import java.net.ServerSocket
import java.net.Socket
import java.io.OutputStream
import java.io.InputStream
import groovy.json.JsonSlurper
import java.util.Random

// Parameters for the TCP server (listener) and remote server (client)
def serverPort = context.getProperty("TCPPort").evaluateAttributeExpressions().getValue() as int

// Start the TCP server (listener)
def serverSocket = new ServerSocket(serverPort)

def sendFailureResponse(socket, receivedMessage, errorMessage) {
    try {
        def trimmedErrorMessage = errorMessage.size() > 150 ? errorMessage.substring(0, 150) + "..." : errorMessage
        
        def random = new Random()
        def randomMessageNumber = random.nextInt(1000000000).toString()
        def failureMessage = """
        {
            "messageNumber": "$randomMessageNumber",
            "status": "failure",
            "statusCode": "1",
            "statusDesc": "Invalid JSON Format: $trimmedErrorMessage",
            "messageTimestamp": "${new Date().format("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", TimeZone.getTimeZone("UTC"))}"
        }
        """
        socket.getOutputStream().write(failureMessage.getBytes("UTF-8"))
        socket.getOutputStream().flush()
        socket.close()  // Close the socket after sending the error message
        log.info("Sent failure response to client and closed the socket: $failureMessage")
    } catch (Exception ex) {
        log.error("Error sending failure response: ${ex.message}", ex)
    }
}

def sendFlowFileContentToClient(socket) {
    // Fetch the FlowFile from the session
    def flowFile = session.get()
    
    // If there's no FlowFile, do nothing
    if (!flowFile) return

    // Read the FlowFile content
    def flowFileContent = new StringBuilder()
    session.read(flowFile, { inputStream ->
        flowFileContent.append(new String(inputStream.getBytes(), "UTF-8"))
    } as InputStreamCallback)

    // Send the FlowFile content to the Hercules client
    socket.getOutputStream().write(flowFileContent.toString().getBytes("UTF-8"))
    socket.getOutputStream().flush()
    log.info("Sent FlowFile content to client: $flowFileContent")

    // Optionally, you can remove the FlowFile from the session after sending its content
    session.remove(flowFile)
}

def handleClient(socket) {
    try {
        def input = socket.getInputStream()
        def output = socket.getOutputStream()

        while (!Thread.currentThread().isInterrupted()) {
            def buffer = new byte[1024]
            int bytesRead = input.read(buffer)
            
            if (bytesRead == -1) {
                log.warn("Client disconnected")
                throw new Exception("Client disconnected")
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

            def messageNumber = json.messageNumber ?: "Unknown"
            def messageType = json.messageType ?: "Unknown"

            def responseMessage = """
            {
                "messageNumber": "$messageNumber",
                "status": "success",
                "statusCode": "0",
                "statusDesc": "Message processed successfully",
                "messageTimestamp": "${new Date().format("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", TimeZone.getTimeZone("UTC"))}"
            }
            """
            output.write(responseMessage.getBytes("UTF-8"))
            output.flush()
            log.error("Sent response to client: $responseMessage")

            // Send the FlowFile content (if available) to the Hercules client
            sendFlowFileContentToClient(socket)
        }
    } catch (Exception e) {
        log.error("Error while handling client connection: ${e.message}", e)
        log.warn("Waiting 10 seconds before accepting the next connection...")
        Thread.sleep(10000)  // Sleep for 10 seconds
        return
    }
}

def sendKeepAlive(socket) {
    while (!Thread.currentThread().isInterrupted()) {
        try {
            def random = new Random()
            def randomMessageNumber = random.nextInt(1000000000).toString()
            def keepAliveMessage = """
            {
                "messageNumber": "$randomMessageNumber",
                "messageType": "KEEPALIVE",
                "messageTimestamp": "${new Date().format("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", TimeZone.getTimeZone("UTC"))}"
            }
            """
            socket.getOutputStream().write(keepAliveMessage.getBytes("UTF-8"))
            socket.getOutputStream().flush()
            log.info("Sent keep-alive message to client")
            sleep(30000) // Sleep for 30 seconds
        } catch (Exception e) {
            log.error("Error while sending keep-alive: ${e.message}", e)
            return
        }
    }
}

def shutdownHook = new Thread({
    log.info("Shutting down server socket...")
    serverSocket.close()
})
Runtime.getRuntime().addShutdownHook(shutdownHook)

while (true) {
    try {
        def clientSocket = serverSocket.accept()
        def keepAliveThread = new Thread({ sendKeepAlive(clientSocket) })
        keepAliveThread.start() // Start keep-alive thread
        handleClient(clientSocket)
    } catch (InterruptedException e) {
        log.info("Script terminated, closing server socket...")
        serverSocket.close() // Close the server socket first
        shutdownHook.join() // Wait for the shutdown hook to complete
        break
    }
}
