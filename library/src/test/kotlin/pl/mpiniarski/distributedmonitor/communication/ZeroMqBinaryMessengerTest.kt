package pl.mpiniarski.distributedmonitor.communication

import org.junit.Assert.assertEquals
import org.junit.Test

internal class ZeroMqBinaryMessengerTest {

    val nodes = listOf(
            "localhost:5557",
            "localhost:5558",
            "localhost:5559"
    )

    @Test
    fun sendAndReceive() {
        val communicator1 = ZeroMqBinaryMessenger(nodes[0], listOf(nodes[1]))
        val communicator2 = ZeroMqBinaryMessenger(nodes[1], listOf(nodes[0]))

        val message = BinaryMessage("header".toByteArray(), "body".toByteArray())
        communicator1.send(nodes[1], message)
        val receivedMessage = communicator2.receive()

        assertEquals(message, receivedMessage)

        communicator1.close()
        communicator2.close()
    }

    @Test
    fun sendToAll() {
        val communicator1 = ZeroMqBinaryMessenger(nodes[0], nodes - nodes[0])
        val communicator2 = ZeroMqBinaryMessenger(nodes[1], nodes - nodes[1])
        val communicator3 = ZeroMqBinaryMessenger(nodes[2], nodes - nodes[2])

        val message = BinaryMessage("header".toByteArray(), "body".toByteArray())

        communicator1.sendToAll(message)

        val receivedMessage2 = communicator2.receive()
        val receivedMessage3 = communicator3.receive()
        assertEquals(receivedMessage2, message)
        assertEquals(receivedMessage3, message)

        communicator1.close()
        communicator2.close()
        communicator3.close()
    }
}