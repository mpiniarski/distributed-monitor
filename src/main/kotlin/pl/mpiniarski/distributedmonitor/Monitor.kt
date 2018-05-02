package pl.mpiniarski.distributedmonitor

import mu.KotlinLogging
import pl.mpiniarski.distributedmonitor.communication.MessageBody
import pl.mpiniarski.distributedmonitor.communication.MessageHeader
import pl.mpiniarski.distributedmonitor.communication.Messenger
import java.util.concurrent.locks.ReentrantLock


class StateMessage(val state : ByteArray, timestamp : Int) : TimestampedMessage(timestamp) {
    override fun serialize() : ByteArray {
        return state.plus(";$timestamp".toByteArray())
    }
}

open class TimestampedMessage(val timestamp : Int) : MessageBody() {
    open fun serialize() : ByteArray {
        return "$timestamp".toByteArray()
    }
}

abstract class DistributedMonitor(private val name : String, private val messenger : Messenger) {
    private val logger = KotlinLogging.logger { }

    companion object {
        const val STATE : String = "0"
    }

    init {
        messenger.addHandler(name) { header : MessageHeader, body : ByteArray ->
            localLock.lock()

            val attributes = String(body).split(";")
            val stateBody = StateMessage(attributes[0].toByteArray(), attributes[1].toInt())

            timeManager.notifySync(stateBody.timestamp, header.sender)
            when (header.type) {
                STATE -> {
                    deserializeAndUpdateState(stateBody.state)
                    logger.debug(logMessage("received STATE"))
                    timeManager.notifyEvent()
                }
            }

            localLock.unlock()
        }
    }

    private fun logMessage(operationName : String) =
            "$name: $operationName: ${timeManager.localTime};${messenger.node}"

    private val localLock = ReentrantLock(true)
    private val timeManager = TimeManager(messenger.nodes)
    protected val lock : DistributedLock = DistributedLock("$name/lock", messenger, localLock, timeManager)

    protected fun createCondition(name : String) : Condition {
        return lock.newCondition(name)
    }

    protected inline fun <T> entry(f : () -> T) : T {
        try {
            lock.lock()
            return f()
        } finally {
            synchronizeState()
            lock.unlock()
        }
    }

    protected fun synchronizeState() {
        localLock.lock()
        messenger.sendToAll(MessageHeader(name, messenger.node, STATE), StateMessage(serializeState(), timeManager.localTime).serialize())
        timeManager.notifyEvent()
        localLock.unlock()
    }

    protected abstract fun serializeState() : ByteArray
    protected abstract fun deserializeAndUpdateState(state : ByteArray)
}

