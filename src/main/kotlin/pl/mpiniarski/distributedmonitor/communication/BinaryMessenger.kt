package pl.mpiniarski.distributedmonitor.communication

import org.zeromq.ZMQ
import org.zeromq.ZMsg
import java.util.*

class AddressNotKnownException : Exception()
class UnableToReceiveException : Exception()


data class BinaryMessage(
        val header : ByteArray,
        val body : ByteArray
) {
    override fun equals(other : Any?) : Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as BinaryMessage

        if (!Arrays.equals(header, other.header)) return false
        if (!Arrays.equals(body, other.body)) return false

        return true
    }

    override fun hashCode() : Int {
        var result = Arrays.hashCode(header)
        result = 31 * result + Arrays.hashCode(body)
        return result
    }
}

interface BinaryMessenger {
    val nodeAddress : String
    val remoteNodesAddresses : List<String>

    fun send(receiver : String, binaryMessage : BinaryMessage)
    fun sendToAll(binaryMessage : BinaryMessage)
    fun receive() : BinaryMessage
    fun close()
}

class ZeroMqBinaryMessenger(override val nodeAddress : String, override val remoteNodesAddresses : List<String>) : BinaryMessenger {
    private val receiveSocket : ZMQ.Socket
    private val sendSockets : Map<String, ZMQ.Socket>

    private val context = ZMQ.context(1)

    init {
        receiveSocket = context.socket(ZMQ.PULL)
        receiveSocket.bind(nodeAddress)

        sendSockets = remoteNodesAddresses.map {
            val socket = context.socket(ZMQ.PUSH)
            socket.connect(it)
            it to socket
        }.toMap()
    }

    override fun send(receiver : String, binaryMessage : BinaryMessage) {
        val socket = sendSockets[receiver] ?: throw AddressNotKnownException()
        val zMsg = prepareZMsg(binaryMessage)
        zMsg.send(socket)
    }

    override fun sendToAll(binaryMessage : BinaryMessage) {
        sendSockets.forEach {
            val zMsg = prepareZMsg(binaryMessage)
            zMsg.send(it.value)
        }
    }

    private fun prepareZMsg(binaryMessage : BinaryMessage) : ZMsg {
        val zMsg = ZMsg()
        zMsg.add(binaryMessage.header)
        zMsg.add(binaryMessage.body)
        return zMsg
    }

    override fun receive() : BinaryMessage {
        val message = ZMsg.recvMsg(receiveSocket)
        val header = message.pop()
        val body = message.pop()
        if (header == null || body == null){
            throw UnableToReceiveException()
        }
        return BinaryMessage(header.data, body.data)
    }

    override fun close() {
        receiveSocket.close()
        sendSockets.forEach { it.value.close() }
        context.term()
    }
}

