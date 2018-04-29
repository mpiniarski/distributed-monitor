package pl.mpiniarski.distributedmonitor

import pl.mpiniarski.distributedmonitor.communication.MessageBody
import pl.mpiniarski.distributedmonitor.communication.MessageHeader
import pl.mpiniarski.distributedmonitor.communication.Messenger
import java.util.concurrent.BlockingQueue
import java.util.concurrent.PriorityBlockingQueue
import java.util.concurrent.Semaphore


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

    companion object {
        const val REQUEST : String = "1"
        const val RESPONSE : String = "2"
        const val RELEASE : String = "3"
    }

    private val node = messenger.node
    private val nodes = messenger.nodes

    init {
        val tryToRelease = {
            val topRequest = queue.element()
            if (topRequest.host == node && timeManager.allRemoteLaterThen(topRequest.priority)) {
                semaphore.release()
            }
        }

        messenger.addHandler(name) { header : MessageHeader, body : ByteArray ->
            val timestampedBody = TimestampedMessage(String(body).toInt())
            timeManager.notifySync(timestampedBody.timestamp, header.sender)
            when (header.type) {
                REQUEST -> {
                    queue.add(Request(timestampedBody.timestamp, header.sender))
                    messenger.send(header.sender, MessageHeader(name, node, RESPONSE), TimestampedMessage(timeManager.localTime).serialize())
                    timeManager.notifyEvent()
                }
                RESPONSE -> {
                    tryToRelease()
                }
                RELEASE -> {
                    queue.remove()
                    if (!queue.isEmpty()) {
                        tryToRelease()
                    }
                }
            }
        }
    }

    private val semaphore = Semaphore(0, true)
    private val queue : BlockingQueue<Request> = PriorityBlockingQueue<Request>()

    private val timeManager = TimeManager(nodes)

    fun lock() {
        queue.add(Request(timeManager.localTime, node))
        messenger.sendToAll(MessageHeader(name, node, REQUEST), TimestampedMessage(timeManager.localTime).serialize())
        timeManager.notifyEvent()
        semaphore.acquire()
    }

    fun unlock() {
        queue.remove()
        messenger.sendToAll(MessageHeader(name, node, RELEASE), TimestampedMessage(timeManager.localTime).serialize())
        timeManager.notifyEvent()
    }

    fun newCondition(name : String) : Condition {
        return Condition("${this.name}/$name", this, messenger, timeManager)
    }
}
