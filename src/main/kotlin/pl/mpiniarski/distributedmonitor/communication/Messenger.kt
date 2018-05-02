package pl.mpiniarski.distributedmonitor.communication

import mu.KotlinLogging
import java.util.*
import kotlin.concurrent.thread


data class MessageHeader(val objectName : String, val sender : String, val type : String)
abstract class MessageBody

class UnsupportedObjectException(objectName : String) : Exception("Message to object [$objectName] is not supported")

interface Messenger {
    val localNode : String
    val remoteNodes : List<String>

    fun send(receiver : String, header : MessageHeader, messageBody : ByteArray)
    fun sendToAll(header : MessageHeader, messageBody : ByteArray)
    fun start()
    fun close()
}

class StandardMessenger(
        private val binaryMessenger : BinaryMessenger
) : Messenger {
    companion object {

        val logger = KotlinLogging.logger { }
    }

    private val handlers : MutableMap<String, (MessageHeader, ByteArray) -> Unit> = HashMap()
    private var standardHandler : (MessageHeader, ByteArray) -> Unit = { _ : MessageHeader, _ : ByteArray -> }

    override val localNode = binaryMessenger.localNode
    override val remoteNodes = binaryMessenger.remoteNodes

    fun setStandardHandler(handler : (MessageHeader, ByteArray) -> Unit) {
        standardHandler = handler
    }

    fun addHandler(objectName : String, handler : (MessageHeader, ByteArray) -> Unit) {
        handlers[objectName] = handler
    }

    override fun send(receiver : String, header : MessageHeader, messageBody : ByteArray) {
        binaryMessenger.send(receiver, BinaryMessage(serializeHeader(header), messageBody))
        logger.debug("Sent ${header.type} to $receiver")
    }

    override fun sendToAll(header : MessageHeader, messageBody : ByteArray) {
        binaryMessenger.sendToAll(BinaryMessage(serializeHeader(header), messageBody))
        logger.debug("Sent ${header.type} to all")
    }

    override fun start() {
        thread(start = true) {
            while (true) {
                try {
                    val binaryMessage = binaryMessenger.receive()

                    val header = deserializeHeader(binaryMessage.header)
                    standardHandler(header, binaryMessage.body)
                    val handler = handlers[header.objectName] ?: throw UnsupportedObjectException(header.type)
                    handler(header, binaryMessage.body)

                } catch (exception : UnableToReceiveException) {
                    logger.warn { "Unable to receive message" }
                }
            }
        }
    }

    override fun close() {
        binaryMessenger.close()
    }

    private fun serializeHeader(header : MessageHeader) : ByteArray {
        return "${header.objectName};${header.sender};${header.type}".toByteArray()
    }

    private fun deserializeHeader(header : ByteArray) : MessageHeader {
        val attributes = String(header).split(";")
        return MessageHeader(attributes[0], attributes[1], attributes[2])
    }
}
