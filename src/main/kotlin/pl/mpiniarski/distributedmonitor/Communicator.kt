package pl.mpiniarski.distributedmonitor

import org.zeromq.ZMQ


class AddressNotKnownException : Exception()

interface Communicator {
    val node : String
    fun send(address : String, message : String)
    fun sendToAll(message : String)
    fun receive() : String
    fun close()

}

class ZeroMqCommunicator(override val node : String, nodes : List<String>) : Communicator {
    private val receiver : ZMQ.Socket
    private val senders : Map<String, ZMQ.Socket>

    private val context = ZMQ.context(1)

    init {

        receiver = context.socket(ZMQ.PULL)
        receiver.bind(node)

        senders = nodes.map {
            val socket = context.socket(ZMQ.PUSH)
            socket.connect(it)
            it to socket
        }.toMap()
    }

    override fun send(address : String, message : String) {
        senders[address]?.send(message) ?: throw AddressNotKnownException()
    }

    override fun sendToAll(message : String) {
        senders.forEach {
            it.value.send(message)
        }
    }

    override fun receive() : String {
        return receiver.recvStr()
    }

    override fun close() {
        receiver.close()
        senders.forEach { it.value.close() }
        context.term()
    }

}

