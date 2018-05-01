package pl.mpiniarski.distributedmonitor

import mu.KotlinLogging
import pl.mpiniarski.distributedmonitor.communication.MessageHeader
import pl.mpiniarski.distributedmonitor.communication.Messenger
import java.util.concurrent.BlockingQueue
import java.util.concurrent.PriorityBlockingQueue
import java.util.concurrent.locks.ReentrantLock

class AwaitRequest(val priority : Int, val host : String) : Comparable<Request> {
    override fun compareTo(other : Request) = when {
        priority < other.priority -> -1
        priority > other.priority -> 1
        host < other.host -> -1
        host > other.host -> 1
        else -> 0
    }
}

class Condition(private val name : String, private val lock : DistributedLock, private val messenger : Messenger,
                private val timeManager : TimeManager,
                private val localLock : ReentrantLock) {
    private val logger = KotlinLogging.logger { }

    companion object {
        const val AWAIT_ON_CONDITION : String = "4"
        const val SIGNAL_ON_CONDITION : String = "5"
    }

    private val node = messenger.node
    private val localCondition = localLock.newCondition()
    private val awaiting : BlockingQueue<AwaitRequest> = PriorityBlockingQueue<AwaitRequest>()

    init {
        messenger.addHandler(name) { header : MessageHeader, body : ByteArray ->
            localLock.lock()

            val timestampedBody = TimestampedMessage(String(body).toInt())
            timeManager.notifySync(timestampedBody.timestamp, header.sender)
            when (header.type) {
                AWAIT_ON_CONDITION -> {
                    awaiting.put(AwaitRequest(timestampedBody.timestamp, header.sender))
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

        awaiting.put(AwaitRequest(timeManager.localTime, node))
        logger.debug(logMessage("await", awaiting))
        messenger.sendToAll(MessageHeader(name, node, AWAIT_ON_CONDITION), TimestampedMessage(timeManager.localTime).serialize())
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
            messenger.sendToAll(MessageHeader(name, node, SIGNAL_ON_CONDITION), TimestampedMessage(timeManager.localTime).serialize())
            timeManager.notifyEvent()
            if (topRequest.host == node) {
                localCondition.signal()
            }
        }

        localLock.unlock()
    }

    private fun logMessage(operationName : String, queue : BlockingQueue<AwaitRequest>) = "$name: $operationName: ${timeManager.localTime};$node;${queue.joinToString(",", "[", "]") { "${it.host}:${it.priority}" }}}"
}