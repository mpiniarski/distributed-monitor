package pl.mpiniarski.distributedmonitor

import pl.mpiniarski.distributedmonitor.communication.*
import java.util.concurrent.BlockingQueue
import java.util.concurrent.PriorityBlockingQueue
import java.util.concurrent.Semaphore
import kotlin.concurrent.thread


class TimestampedMessage(val timestamp : Int) : MessageBody()

class Request(val priority : Int, val host : String) : Comparable<Request> {
    override fun compareTo(other : Request) = when {
        priority < other.priority -> -1
        priority > other.priority -> 1
        host < other.host -> -1
        host > other.host -> 1
        else -> 0
    }
}

class DistributedLock(private val node : String, nodes : List<String>, binaryMessenger : BinaryMessenger) {

    companion object {
        const val REQUEST : String = "1"
        const val RESPONSE : String = "2"
        const val RELEASE : String = "3"
    }

    private val messenger = Messenger(
            listOf(
                    BodySerializer(REQUEST, { val msg = it as TimestampedMessage; "${msg.timestamp}" }, { TimestampedMessage(it.toInt()) }),
                    BodySerializer(RESPONSE, { val msg = it as TimestampedMessage; "${msg.timestamp}" }, { TimestampedMessage(it.toInt()) }),
                    BodySerializer(RELEASE, { val msg = it as TimestampedMessage; "${msg.timestamp}" }, { TimestampedMessage(it.toInt()) })
            ),
            binaryMessenger
    )
    private val semaphore = Semaphore(0, true)
    private val queue : BlockingQueue<Request> = PriorityBlockingQueue<Request>()

    private val timeManager = TimeManager(nodes)

    init {
        val tryToRelease = {
            val topRequest = queue.element()
            if (topRequest.host == node && timeManager.allRemoteLaterThen(topRequest.priority)) {
                semaphore.release()
            }
        }
        thread(start = true) {
            while (true) {
                val message = messenger.receive()
                val header = message.header
                val body = message.body as TimestampedMessage
                timeManager.notifySync(body.timestamp, header.sender)
                when (header.type) {
                    REQUEST -> {
                        queue.add(Request(body.timestamp, header.sender))
                        messenger.send(header.sender, Message(MessageHeader(node, RESPONSE), TimestampedMessage(timeManager.localTime)))
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
    }

    fun lock() {
        queue.add(Request(timeManager.localTime, node))
        messenger.sendToAll(Message(MessageHeader(node, REQUEST), TimestampedMessage(timeManager.localTime)))
        timeManager.notifyEvent()
        semaphore.acquire()
    }

    fun unlock() {
        queue.remove()
        messenger.sendToAll(Message(MessageHeader(node, RELEASE), TimestampedMessage(timeManager.localTime)))
        timeManager.notifyEvent()
    }

    fun newCondition() : DistributedCondition {
        //TODO
        return DistributedCondition()
    }
}
