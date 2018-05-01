package pl.mpiniarski.distributedmonitor

import mu.KotlinLogging
import pl.mpiniarski.distributedmonitor.communication.MessageBody
import pl.mpiniarski.distributedmonitor.communication.MessageHeader
import pl.mpiniarski.distributedmonitor.communication.Messenger
import java.util.concurrent.BlockingQueue
import java.util.concurrent.PriorityBlockingQueue
import java.util.concurrent.locks.ReentrantLock


open class TimestampedMessage(val timestamp : Int) : MessageBody() {
    fun serialize() : ByteArray {
        return "$timestamp".toByteArray()
    }
}

class Request(val priority : Int, val host : String) : Comparable<Request> {
    override fun compareTo(other : Request) = when {
        priority < other.priority -> -1
        priority > other.priority -> 1
        host < other.host -> -1
        host > other.host -> 1
        else -> 0
    }
}

class DistributedLock(private val name : String, private val messenger : Messenger) {
    private val logger = KotlinLogging.logger { }

    companion object {
        const val REQUEST : String = "1"
        const val RESPONSE : String = "2"
        const val RELEASE : String = "3"
    }

    private val node = messenger.node
    private val nodes = messenger.nodes

    private val queue : BlockingQueue<Request> = PriorityBlockingQueue<Request>()

    private val localLock = ReentrantLock(true)
    private val localCondition = localLock.newCondition()

    private val timeManager = TimeManager(nodes)

    init {
        val tryToRelease = {
            val topRequest = queue.peek()
            if (topRequest != null && topRequest.host == node && timeManager.allRemoteLaterThen(topRequest.priority)) {
                localCondition.signal()
            }
        }

        messenger.addHandler(name) { header : MessageHeader, body : ByteArray ->
            localLock.lock()

            val timestampedBody = TimestampedMessage(String(body).toInt())
            timeManager.notifySync(timestampedBody.timestamp, header.sender)
            when (header.type) {
                REQUEST -> {
                    queue.add(Request(timestampedBody.timestamp, header.sender))
                    logger.debug(logMessage("received REQUEST", queue))
                    messenger.send(header.sender, MessageHeader(name, node, RESPONSE), TimestampedMessage(timeManager.localTime).serialize())
                    timeManager.notifyEvent()
                }
                RESPONSE -> {
                    logger.debug(logMessage("received RESPONSE", queue))
                    tryToRelease()
                }
                RELEASE -> {
                    queue.remove()
                    logger.debug(logMessage("received RELEASE", queue))
                    if (!queue.isEmpty()) {
                        tryToRelease()
                    }
                }
            }

            localLock.unlock()
        }
    }


    fun lock() {
        localLock.lock()

        queue.add(Request(timeManager.localTime, node))
        logger.debug(logMessage("lock", queue))
        messenger.sendToAll(MessageHeader(name, node, REQUEST), TimestampedMessage(timeManager.localTime).serialize())
        timeManager.notifyEvent()
        localCondition.await()

        logger.debug("$name: ENTER CS: ${timeManager.localTime};$node")
        localLock.unlock()
    }

    fun unlock() {
        localLock.lock()
        logger.debug("$name: LEAVE CS: ${timeManager.localTime};$node")

        queue.remove()
        logger.debug(logMessage("unlock", queue))
        messenger.sendToAll(MessageHeader(name, node, RELEASE), TimestampedMessage(timeManager.localTime).serialize())
        timeManager.notifyEvent()

        localLock.unlock()
    }

    private fun logMessage(operationName : String, queue : BlockingQueue<Request>) =
            "$name: $operationName: ${timeManager.localTime};$node;${queue.joinToString(",", "[", "]") { "${it.host}:${it.priority}" }}}"

    fun newCondition(name : String) : Condition {
        return Condition("${this.name}/$name", this, messenger, timeManager, localLock)
    }
}
