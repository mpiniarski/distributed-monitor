package pl.mpiniarski.distributedmonitor

import mu.KotlinLogging
import pl.mpiniarski.distributedmonitor.communication.MessageHeader
import pl.mpiniarski.distributedmonitor.communication.Messenger
import java.util.*
import java.util.concurrent.locks.Lock

class ConditionMessageBody(val sequenceNumber : Int, timestamp : Int) : TimestampedMessageBody(timestamp) {
    companion object {
        fun deserialize(byteArray : ByteArray) : ConditionMessageBody {
            val attributes = String(byteArray).split(";")
            return ConditionMessageBody(attributes[0].toInt(), attributes[1].toInt())
        }
    }

    override fun serialize() : ByteArray {
        return "$sequenceNumber;".toByteArray().plus(super.serialize())
    }
}

class MyComparator : Comparator<Pair<MessageHeader, ConditionMessageBody>> {
    override fun compare(p0 : Pair<MessageHeader, ConditionMessageBody>, p1 : Pair<MessageHeader, ConditionMessageBody>) : Int =
            when {
                p0.second.sequenceNumber < p1.second.sequenceNumber -> -1
                p0.second.sequenceNumber > p1.second.sequenceNumber -> 1
                else -> 0
            }
}

class DistributedCondition(private val name : String, private val lock : DistributedLock, private val messenger : Messenger,
                           private val timeManager : TimeManager,
                           private val localLock : Lock) {
    companion object {
        val logger = KotlinLogging.logger { }
        const val AWAIT_ON_CONDITION : String = "4"
        const val SIGNAL_ON_CONDITION : String = "5"
    }

    private val node = messenger.localNode
    private val localCondition = localLock.newCondition()
    private val awaiting : Queue<Request> = PriorityQueue()

    private var sequenceNumber = 0

    private val blockedMessages : Queue<Pair<MessageHeader, ConditionMessageBody>> = PriorityQueue(MyComparator())


    init {
        messenger.addHandler(name) { header : MessageHeader, body : ByteArray ->
            localLock.lock()

            val conditionMessageBody = ConditionMessageBody.deserialize(body)

            if (conditionMessageBody.sequenceNumber == sequenceNumber) {
                sequenceNumber += 1
                handleMessage(header, conditionMessageBody)
                while (blockedMessages.isNotEmpty() && blockedMessages.element().second.sequenceNumber == sequenceNumber) {
                    sequenceNumber += 1
                    val messageToHandle = blockedMessages.remove()
                    handleMessage(messageToHandle.first, messageToHandle.second)
                }
            } else {
                blockedMessages.add(Pair(header, conditionMessageBody))
            }

            localLock.unlock()
        }
    }

    private fun handleMessage(header : MessageHeader, conditionMessageBody : ConditionMessageBody) {
        timeManager.notifySync(conditionMessageBody.timestamp, header.sender)
        when (header.type) {
            AWAIT_ON_CONDITION -> {
                awaiting.add(Request(conditionMessageBody.timestamp, header.sender))
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
    }

    fun await() {
        localLock.lock()

        awaiting.add(Request(timeManager.localTime, node))
        logger.debug(logMessage("await", awaiting))
        messenger.sendToAll(MessageHeader(name, node, AWAIT_ON_CONDITION), ConditionMessageBody(sequenceNumber, timeManager.localTime).serialize())
        sequenceNumber += 1
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
            messenger.sendToAll(MessageHeader(name, node, SIGNAL_ON_CONDITION), ConditionMessageBody(sequenceNumber, timeManager.localTime).serialize())
            sequenceNumber += 1
            timeManager.notifyEvent()
            if (topRequest.host == node) {
                localCondition.signal()
            }
        }

        localLock.unlock()
    }

    private fun logMessage(operationName : String, queue : Queue<Request>) =
            "\t$node\t${timeManager.localTime}\t$name\t$operationName\t${queue.joinToString(",", "[", "]") { "${it.host}:${it.priority}" }}}"
}