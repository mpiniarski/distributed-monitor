package pl.mpiniarski.distributedmonitor.communication

import junit.framework.Assert.assertEquals
import org.junit.Test

class MessengerTest {
    val nodes = listOf(
            "tcp://localhost:5557",
            "tcp://localhost:5558",
            "tcp://localhost:5559"
    )

    @Test
    fun sendAndReceive() {
        val communicator1 = ZeroMqCommunicator(
                nodes[0],
                listOf(nodes[1]))

        val communicator2 = ZeroMqCommunicator(
                nodes[1],
                listOf(nodes[0]))

        class TestMessage : Message("TYPE")
        val messageTypes = listOf(MessageType("TYPE", { "" }, { TestMessage() }))

        val messenger1 = Messenger(messageTypes, communicator1)
        val messenger2 = Messenger(messageTypes, communicator2)

        val message = TestMessage()
        messenger1.send(nodes[1], message)
        val (sender, received) = messenger2.receive()

        assertEquals(nodes[0], sender)
        assertEquals(message.type, received.type)
    }
}
