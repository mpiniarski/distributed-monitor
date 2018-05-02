package pl.mpiniarski.distributedmonitor.communication

import junit.framework.Assert.assertEquals
import org.junit.Test
import java.util.concurrent.CountDownLatch

class StandardMessengerTest {
    private val nodes = listOf(
            "tcp://localhost:5557",
            "tcp://localhost:5558"
    )

    @Test
    fun sendAndReceive() {
        val latch = CountDownLatch(1)
        val binaryMessenger1 = ZeroMqBinaryMessenger(nodes[0], nodes - nodes[0])
        val binaryMessenger2 = ZeroMqBinaryMessenger(nodes[1], nodes - nodes[1])

        val messenger1 = StandardMessenger(binaryMessenger1)
        val messenger2 = StandardMessenger(binaryMessenger2)

        val objectName = "name"
        val sendMessageHeader = MessageHeader(objectName, "sender", "type")
        val sendMessageBody = "body".toByteArray()

        var receivedMessageHeader : MessageHeader? = null
        var receivedMessageBody : ByteArray? = null
        messenger1.addHandler(objectName, { header : MessageHeader, body : ByteArray ->
            receivedMessageHeader = header
            receivedMessageBody = body
            latch.countDown()
        })
        messenger1.start()
        messenger2.start()

        messenger2.send(nodes[0], sendMessageHeader, sendMessageBody)

        latch.await()
        assertEquals(sendMessageHeader, receivedMessageHeader)
        assertEquals(String(sendMessageBody), String(receivedMessageBody!!))

        messenger1.close()
        messenger2.close()

    }
}
