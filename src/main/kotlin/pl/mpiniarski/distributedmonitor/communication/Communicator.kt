package pl.mpiniarski.distributedmonitor.communication

import org.zeromq.ZMQ
import org.zeromq.ZMsg

class AddressNotKnownException : Exception()


data class ReceiveResult(
        val sender : String,
        val type : String,
        val payload : String
)

interface Communicator {
    fun send(receiver : String, type : String, payload : String)
    fun sendToAll(type : String, payload : String)
    fun receive() : ReceiveResult
    fun close()
}

class ZeroMqCommunicator(private val node : String, nodes : List<String>) : Communicator {
    private val receiveSocket : ZMQ.Socket
    private val sendSockets : Map<String, ZMQ.Socket>

    private val context = ZMQ.context(1)

    init {
        receiveSocket = context.socket(ZMQ.PULL)
        receiveSocket.bind(node)

        sendSockets = nodes.map {
            val socket = context.socket(ZMQ.PUSH)
            socket.connect(it)
            it to socket
        }.toMap()
    }

    override fun send(receiver : String, type : String, payload : String) {
        val socket = sendSockets[receiver] ?: throw AddressNotKnownException()
        val zMsg = prepareZMsg(type, payload)
        zMsg.send(socket)
    }

    override fun sendToAll(type : String, payload : String) {
        sendSockets.forEach {
            val zMsg = prepareZMsg(type, payload)
            zMsg.send(it.value)
        }
    }

    private fun prepareZMsg(type : String, payload : String) : ZMsg {
        val zMsg = ZMsg()
        zMsg.add(node)
        zMsg.add(type)
        zMsg.add(payload)
        return zMsg
    }

    override fun receive() : ReceiveResult {
        val message = ZMsg.recvMsg(receiveSocket)
        val sender = message.popString()
        val type = message.popString()
        val payload = message.popString()
        return ReceiveResult(sender, type, payload)
    }

    override fun close() {
        receiveSocket.close()
        sendSockets.forEach { it.value.close() }
        context.term()
    }
}

