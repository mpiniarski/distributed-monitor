package pl.mpiniarski.distributedmonitor.communication

import junit.framework.Assert.assertEquals
import org.junit.Test

class MessengerTest {
    private val nodes = listOf(
            "tcp://localhost:5557",
            "tcp://localhost:5558"
    )

    @Test
    fun sendAndReceive() {
        val communicator1 = ZeroMqBinaryMessenger(nodes[0], nodes - nodes[0])
        val communicator2 = ZeroMqBinaryMessenger(nodes[1], nodes - nodes[1])

        class TestMessage(val payload : String) : MessageBody() {
            override fun equals(other : Any?) : Boolean {
                if (this === other) return true
                if (javaClass != other?.javaClass) return false

                other as TestMessage

                if (payload != other.payload) return false

                return true
            }

            override fun hashCode() : Int {
                return payload.hashCode()
            }
        }

        val messageTypes = listOf(BodySerializer("TYPE", { (it as TestMessage).payload }, { TestMessage(it) }))

        val messenger1 = Messenger(messageTypes, communicator1)
        val messenger2 = Messenger(messageTypes, communicator2)

        val message = Message(MessageHeader("1", "TYPE"), TestMessage("payload"))
        messenger1.send(nodes[1], message)
        val receivedMessage = messenger2.receive()

        assertEquals(message, receivedMessage)
    }
}
