package pl.mpiniarski.distributedmonitor.communication


data class MessageHeader(val sender : String, val type : String)
abstract class MessageBody

data class Message(val header : MessageHeader, val body : MessageBody)

class UnsupportedMessageTypeException(type : String) : Exception("Message messageType [$type] is not supported")

class BodySerializer(val messageType : String, val serialize : (MessageBody) -> String, val deserialize : (String) -> MessageBody)

class Messenger(bodySerializers : List<BodySerializer>, private val binaryMessenger : BinaryMessenger) {
    private val serializers = bodySerializers.map { it.messageType to it.serialize }.toMap()
    private val deserializers = bodySerializers.map { it.messageType to it.deserialize }.toMap()

    private fun serializeHeader(header : MessageHeader) : ByteArray {
        return "${header.sender};${header.type}".toByteArray()
    }

    private fun deserializeHeader(header : ByteArray) : MessageHeader {
        val attributes = String(header).split(";")
        return MessageHeader(attributes[0], attributes[1])
    }

    fun send(receiver : String, message : Message) {
        val type = message.header.type
        val serializedMessage = serializers[type]?.let { it(message.body) }
                ?: throw UnsupportedMessageTypeException(type)
        binaryMessenger.send(receiver, BinaryMessage(
                serializeHeader(message.header),
                serializedMessage.toByteArray())
        )
    }

    fun sendToAll(message : Message) {
        val type = message.header.type
        val serializedMessage = serializers[type]?.let { it(message.body) }
                ?: throw UnsupportedMessageTypeException(type)
        binaryMessenger.sendToAll(BinaryMessage(
                serializeHeader(message.header),
                serializedMessage.toByteArray())
        )
    }

    fun receive() : Message {
        val binaryMessage = binaryMessenger.receive()
        val header = deserializeHeader(binaryMessage.header)

        val body = deserializers[header.type]?.let { it(String(binaryMessage.body)) }
                ?: throw UnsupportedMessageTypeException(header.type)
        return Message(header, body)
    }

    fun close() {
        binaryMessenger.close()
    }
}
