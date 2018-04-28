package pl.mpiniarski.distributedmonitor.communication

import org.junit.Assert.assertEquals
import org.junit.Test

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

        val type = "Type1"
        val payload = "message"
        communicator1.send(nodes[1], type, payload)
        val receivedMessage = communicator2.receive()

        assertEquals(type, receivedMessage.type)
        assertEquals(payload, receivedMessage.payload)
        assertEquals(nodes[0], receivedMessage.sender)

        communicator1.close()
        communicator2.close()
    }

    @Test
    fun sendToAll() {
        val communicator1 = ZeroMqCommunicator(
                nodes[0],
                listOf(nodes[1], nodes[2]))

        val communicator2 = ZeroMqCommunicator(
                nodes[1],
                listOf(nodes[0], nodes[2]))

        val communicator3 = ZeroMqCommunicator(
                nodes[2],
                listOf(nodes[0], nodes[1]))

        val type = "TYPE"
        val payload = "PAYLOAD"
        communicator1.sendToAll(type, payload)

        val receivedMessage2 = communicator2.receive()
        assertEquals(type, receivedMessage2.type)
        assertEquals(payload, receivedMessage2.payload)
        assertEquals(nodes[0], receivedMessage2.sender)

        val receivedMessage3 = communicator3.receive()
        assertEquals(type, receivedMessage3.type)
        assertEquals(payload, receivedMessage3.payload)
        assertEquals(nodes[0], receivedMessage3.sender)
    }
    //TODO
//    @Test(expected = TestTimedOutException::class, timeout = 1000)
//    fun shouldNotReceiveWhenSendToAnother() {
//        val communicator1 = ZeroMqCommunicator(
//                nodes[0],
//                listOf(nodes[1], nodes[2]))
//
//        val communicator2 = ZeroMqCommunicator(
//                nodes[1],
//                listOf(nodes[0], nodes[2]))
//
//        val communicator3 = ZeroMqCommunicator(
//                nodes[2],
//                listOf(nodes[0], nodes[1]))
//
//        val sendMessage = Message("TYPE1", "Body 1")
//        communicator1.send(nodes[2], sendMessage)
//
//        communicator2.receive()
//
//        communicator1.close()
//        communicator2.close()
//        communicator3.close()
//    }
}