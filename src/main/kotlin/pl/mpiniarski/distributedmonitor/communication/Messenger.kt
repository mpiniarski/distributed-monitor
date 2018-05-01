package pl.mpiniarski.distributedmonitor.communication

import mu.KotlinLogging
import kotlin.concurrent.thread


data class MessageHeader(val objectName : String, val sender : String, val type : String)
abstract class MessageBody

class UnsupportedObjectException(objectName : String) : Exception("Message to object [$objectName] is not supported")

class Messenger(private val binaryMessenger : BinaryMessenger) {
    private val logger = KotlinLogging.logger { }

    public val node = binaryMessenger.nodeAddress
    public val nodes = binaryMessenger.remoteNodesAddresses

    private val handlers : MutableMap<String, (MessageHeader, ByteArray) -> Unit> = HashMap()

    public fun addHandler(objectName : String, handler : (MessageHeader, ByteArray) -> Unit) {
        handlers[objectName] = handler
    }

    private fun serializeHeader(header : MessageHeader) : ByteArray {
        return "${header.objectName};${header.sender};${header.type}".toByteArray()
    }

    private fun deserializeHeader(header : ByteArray) : MessageHeader {
        val attributes = String(header).split(";")
        return MessageHeader(attributes[0], attributes[1], attributes[2])
    }

    fun send(receiver : String, header : MessageHeader, messageBody : ByteArray) {
        binaryMessenger.send(receiver, BinaryMessage(serializeHeader(header), messageBody))
        logger.debug("Sent ${header.type} to $receiver")
    }

    fun sendToAll(header : MessageHeader, messageBody : ByteArray) {
        binaryMessenger.sendToAll(BinaryMessage(serializeHeader(header), messageBody))
        logger.debug("Sent ${header.type} to all")
    }

    fun start() {
        thread(start = true) {
            while (true) {
                try {
                    val binaryMessage = binaryMessenger.receive()
                    val header = deserializeHeader(binaryMessage.header)
                    val handler = handlers[header.objectName] ?: throw UnsupportedObjectException(header.type)
                    handler(header, binaryMessage.body)
                } catch (exception : UnableToReceiveException) {
                    logger.warn { "Unable to receive message" }
                }
            }
        }
    }

    fun close() {
        binaryMessenger.close()
    }

}
