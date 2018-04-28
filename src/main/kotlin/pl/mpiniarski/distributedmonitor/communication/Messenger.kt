package pl.mpiniarski.distributedmonitor.communication


abstract class Message(val type : String)

class UnsupportedMessageTypeException(type : String) : Exception("Message type [$type] is not supported")

class MessageType(val type : String, val serialize : (Message) -> String, val deserialize : (String) -> Message)

class Messenger(messageTypes : List<MessageType>, private val communicator : Communicator) {
    private val serializers = messageTypes.map { it.type to it.serialize }.toMap()
    private val deserializers = messageTypes.map { it.type to it.deserialize }.toMap()

    fun send(receiver : String, message : Message) {
        val serializedMessage = serializers[message.type]?.let { it(message) }
                ?: throw UnsupportedMessageTypeException(message.type)
        communicator.send(receiver, message.type, serializedMessage)
    }

    fun sendToAll(message : Message) {
        val serializedMessage = serializers[message.type]?.let { it(message) }
                ?: throw UnsupportedMessageTypeException(message.type)
        communicator.sendToAll(message.type, serializedMessage)
    }

    fun receive() : Pair<String, Message> {
        val receive = communicator.receive()
        val deserializedMessage = deserializers[receive.type]?.let { it(receive.payload) }
                ?: throw UnsupportedMessageTypeException(receive.type)
        return Pair(receive.sender, deserializedMessage)
    }

    fun close() {
        communicator.close()
    }
}
