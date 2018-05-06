package pl.mpiniarski.distributedmonitor

import mu.KotlinLogging
import pl.mpiniarski.distributedmonitor.communication.MessageBody
import pl.mpiniarski.distributedmonitor.communication.MessageHeader
import pl.mpiniarski.distributedmonitor.communication.Messenger
import java.util.concurrent.locks.ReentrantLock


open class TimestampedMessageBody(val timestamp : Int) : MessageBody() {
    companion object {
        fun deserialize(byteArray : ByteArray) : TimestampedMessageBody {
            return TimestampedMessageBody(String(byteArray).toInt())
        }
    }

    open fun serialize() : ByteArray {
        return "$timestamp".toByteArray()
    }
}

class StateMessageBody(val state : ByteArray, timestamp : Int) : TimestampedMessageBody(timestamp) {
    companion object {
        fun deserialize(byteArray : ByteArray) : StateMessageBody {
            val attributes = String(byteArray).split(";")
            return StateMessageBody(attributes[0].toByteArray(), attributes[1].toInt())
        }
    }

    override fun serialize() : ByteArray {
        return state.plus(";".toByteArray()).plus(super.serialize())
    }
}

abstract class DistributedMonitor(
        private val name : String,
        private val messenger : Messenger) {
    companion object {
        private val logger = KotlinLogging.logger { }
        const val STATE : String = "0"
    }

    protected abstract fun serializeState() : ByteArray
    protected abstract fun deserializeAndUpdateState(state : ByteArray)

    private val localLock = ReentrantLock(true)
    private val timeManager = TimeManager(messenger.remoteNodes)
    protected val distributedLock : DistributedLock = DistributedLock("$name/lock", messenger, localLock, timeManager)

    init {
        messenger.addHandler(name) { header : MessageHeader, body : ByteArray ->
            localLock.lock()
            val stateBody = StateMessageBody.deserialize(body)
            timeManager.notifySync(stateBody.timestamp, header.sender)
            when (header.type) {
                STATE -> {
                    deserializeAndUpdateState(stateBody.state)
                    logger.debug { logMessage("received STATE") }
                    timeManager.notifyEvent()
                }
            }

            localLock.unlock()
        }
    }

    fun close() {
        messenger.close()
    }

    protected inline fun <T> entry(f : () -> T) : T {
        try {
            distributedLock.lock()
            return f()
        } finally {
            synchronizeState()
            distributedLock.unlock()
        }
    }

    protected fun synchronizeState() {
        localLock.lock()
        timeManager.notifyEvent()
        messenger.sendToAll(MessageHeader(name, messenger.localNode, STATE), StateMessageBody(serializeState(), timeManager.localTime).serialize())
        timeManager.notifyEvent()
        localLock.unlock()
    }

    protected fun createCondition(name : String) : DistributedCondition {
        return distributedLock.newCondition(name)
    }


    private fun logMessage(operationName : String) =
            "\t$name: \t$operationName: \t${timeManager.localTime};\t${messenger.localNode}"
}

