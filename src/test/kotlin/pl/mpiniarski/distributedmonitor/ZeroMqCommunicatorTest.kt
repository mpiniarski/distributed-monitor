package pl.mpiniarski.distributedmonitor

import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.runners.model.TestTimedOutException

internal class ZeroMqCommunicatorTest {

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

        val sendMessage = "message1"
        communicator1.send(nodes[1], sendMessage)
        val receivedMessage = communicator2.receive()

        assertEquals(sendMessage, receivedMessage)

        communicator1.close()
        communicator2.close()
    }

    @Test(expected = TestTimedOutException::class, timeout = 1000)
    fun shouldNotReceiveWhenSendToAnother() {
        val communicator1 = ZeroMqCommunicator(
                nodes[0],
                listOf(nodes[1], nodes[2]))

        val communicator2 = ZeroMqCommunicator(
                nodes[1],
                listOf(nodes[0], nodes[2]))

        val communicator3 = ZeroMqCommunicator(
                nodes[2],
                listOf(nodes[0], nodes[1]))

        val sendMessage = "message1"
        communicator1.send(nodes[2], sendMessage)

        communicator2.receive()

        communicator1.close()
        communicator2.close()
        communicator3.close()
    }
}