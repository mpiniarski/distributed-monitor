package pl.mpiniarski.distributedmonitor

import mu.KotlinLogging
import pl.mpiniarski.distributedmonitor.communication.MessageHeader
import pl.mpiniarski.distributedmonitor.communication.StandardMessenger
import java.util.*
import java.util.concurrent.locks.ReentrantLock

class Condition(private val name : String, private val lock : DistributedLock, private val messenger : StandardMessenger,
                private val timeManager : TimeManager,
                private val localLock : ReentrantLock) {
    companion object {
        val logger = KotlinLogging.logger { }
        const val AWAIT_ON_CONDITION : String = "4"
        const val SIGNAL_ON_CONDITION : String = "5"
    }

    private val node = messenger.localNode
    private val localCondition = localLock.newCondition()
    private val awaiting : Queue<Request> = PriorityQueue<Request>()

    init {
        messenger.addHandler(name) { header : MessageHeader, body : ByteArray ->
            localLock.lock()

            val timestampedBody = TimestampedMessageBody.deserialize(body)
            timeManager.notifySync(timestampedBody.timestamp, header.sender)
            when (header.type) {
                AWAIT_ON_CONDITION -> {
                    awaiting.add(Request(timestampedBody.timestamp, header.sender))
                    logger.debug(logMessage("received AWAIT_ON_CONDITION", awaiting))
                }
                SIGNAL_ON_CONDITION -> {
                    val topRequest = awaiting.remove()
                    logger.debug(logMessage("received SIGNAL_ON_CONDITION", awaiting))
                    if (topRequest.host == node) {
                        localCondition.signal()
                    }
                }
            }

            localLock.unlock()
        }

    }

    fun await() {
        localLock.lock()

        awaiting.add(Request(timeManager.localTime, node))
        logger.debug(logMessage("await", awaiting))
        messenger.sendToAll(MessageHeader(name, node, AWAIT_ON_CONDITION), TimestampedMessageBody(timeManager.localTime).serialize())
        timeManager.notifyEvent()

        lock.unlock()
        localCondition.await()
        lock.lock()

        localLock.unlock()
    }

    fun signal() {
        localLock.lock()

        if (!awaiting.isEmpty()) {
            val topRequest = awaiting.remove()
            logger.debug(logMessage("signal", awaiting))
            messenger.sendToAll(MessageHeader(name, node, SIGNAL_ON_CONDITION), TimestampedMessageBody(timeManager.localTime).serialize())
            timeManager.notifyEvent()
            if (topRequest.host == node) {
                localCondition.signal()
            }
        }

        localLock.unlock()
    }

    private fun logMessage(operationName : String, queue : Queue<Request>) =
            "$name: $operationName: ${timeManager.localTime};$node;${queue.joinToString(",", "[", "]") { "${it.host}:${it.priority}" }}}"
}