package pl.mpiniarski.distributedmonitor

import mu.KotlinLogging
import pl.mpiniarski.distributedmonitor.communication.MessageHeader
import pl.mpiniarski.distributedmonitor.communication.Messenger
import java.util.*
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock


class Request(val priority : Int, val host : String) : Comparable<Request> {
    override fun compareTo(other : Request) = when {
        priority < other.priority -> -1
        priority > other.priority -> 1
        host < other.host -> -1
        host > other.host -> 1
        else -> 0
    }
}

class DistributedLock(private val name : String,
                      private val messenger : Messenger,
                      private val localLock : Lock = ReentrantLock(),
                      private val timeManager : TimeManager = TimeManager(messenger.remoteNodes)) {
    companion object {
        private val logger = KotlinLogging.logger { }
        const val REQUEST : String = "1"
        const val RESPONSE : String = "2"
        const val RELEASE : String = "3"
    }

    private val localCondition = localLock.newCondition()
    private val requestQueue : Queue<Request> = PriorityQueue()

    init {
        val tryToRelease = {
            val topRequest = requestQueue.peek()
            if (topRequest != null && topRequest.host == messenger.localNode && timeManager.allRemoteLaterThen(topRequest.priority)) {
                localCondition.signal()
            }
        }

        messenger.addHandler(name) { header : MessageHeader, body : ByteArray ->
            localLock.lock()

            val timestampedBody = TimestampedMessageBody.deserialize(body)
            timeManager.notifySync(timestampedBody.timestamp, header.sender)
            when (header.type) {
                REQUEST -> {
                    requestQueue.add(Request(timestampedBody.timestamp, header.sender))
                    logger.debug(logMessage("received REQUEST", requestQueue))
                    messenger.send(header.sender, MessageHeader(name, messenger.localNode, RESPONSE), TimestampedMessageBody(timeManager.localTime).serialize())
                    timeManager.notifyEvent()
                }
                RESPONSE -> {
                    logger.debug(logMessage("received RESPONSE", requestQueue))
                    tryToRelease()
                }
                RELEASE -> {
                    requestQueue.remove()
                    logger.debug(logMessage("received RELEASE", requestQueue))
                    if (!requestQueue.isEmpty()) {
                        tryToRelease()
                    }
                }
            }

            localLock.unlock()
        }
    }


    fun lock() {
        localLock.lock()

        requestQueue.add(Request(timeManager.localTime, messenger.localNode))
        logger.debug(logMessage("distributedLock", requestQueue))
        messenger.sendToAll(MessageHeader(name, messenger.localNode, REQUEST), TimestampedMessageBody(timeManager.localTime).serialize())
        timeManager.notifyEvent()
        localCondition.await()

        logger.debug("$name: ENTER CS: ${timeManager.localTime};${messenger.localNode}")
        localLock.unlock()
    }

    fun unlock() {
        localLock.lock()
        logger.debug("$name: LEAVE CS: ${timeManager.localTime};${messenger.localNode}")

        requestQueue.remove()
        logger.debug(logMessage("unlock", requestQueue))
        messenger.sendToAll(MessageHeader(name, messenger.localNode, RELEASE), TimestampedMessageBody(timeManager.localTime).serialize())
        timeManager.notifyEvent()

        localLock.unlock()
    }

    fun newCondition(name : String) : DistributedCondition {
        return DistributedCondition("${this.name}/$name", this, messenger, timeManager, localLock)
    }

    private fun logMessage(operationName : String, queue : Queue<Request>) =
            "$name: $operationName: ${timeManager.localTime};${messenger.localNode};${queue.joinToString(",", "[", "]") { "${it.host}:${it.priority}" }}}"
}
