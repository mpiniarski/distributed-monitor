package pl.mpiniarski.distributedmonitor

import pl.mpiniarski.distributedmonitor.communication.MessageHeader
import pl.mpiniarski.distributedmonitor.communication.Messenger
import java.util.concurrent.BlockingQueue
import java.util.concurrent.PriorityBlockingQueue
import java.util.concurrent.Semaphore

class AwaitRequest(private val priority : Int, val host : String) : Comparable<Request> {
    override fun compareTo(other : Request) = when {
        priority < other.priority -> -1
        priority > other.priority -> 1
        host < other.host -> -1
        host > other.host -> 1
        else -> 0
    }
}

class Condition(private val name : String, private val lock : DistributedLock, private val messenger : Messenger,
                private val timeManager : TimeManager) {

    companion object {
        const val AWAIT_ON_CONDITION : String = "4"
        const val SIGNAL_ON_CONDITION : String = "5"
    }

    private val node = messenger.node

    init {
        messenger.addHandler(name) { header : MessageHeader, body : ByteArray ->
            val timestampedBody = TimestampedMessage(String(body).toInt())
            timeManager.notifySync(timestampedBody.timestamp, header.sender)
            when (header.type) {
                AWAIT_ON_CONDITION -> {
                    awaiting.put(AwaitRequest(timestampedBody.timestamp, header.sender))
                }
                SIGNAL_ON_CONDITION -> {
                    val topRequest = awaiting.remove()
                    if (topRequest.host == node) {
                        semaphore.release()
                    }
                }
            }
        }

    }

    private val semaphore = Semaphore(0, true)
    private val awaiting : BlockingQueue<AwaitRequest> = PriorityBlockingQueue<AwaitRequest>()

    fun await() {
        awaiting.put(AwaitRequest(timeManager.localTime, node))
        messenger.sendToAll(MessageHeader(name, node, AWAIT_ON_CONDITION), TimestampedMessage(timeManager.localTime).serialize())
        timeManager.notifyEvent()
        lock.unlock()
        semaphore.acquire()
        lock.lock()
    }

    fun signal() {
        if (!awaiting.isEmpty()) {
            val topRequest = awaiting.remove()
            messenger.sendToAll(MessageHeader(name, node, SIGNAL_ON_CONDITION), TimestampedMessage(timeManager.localTime).serialize())
            if (topRequest.host == node) {
                semaphore.release()
            }
        }
    }
}