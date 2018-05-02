package pl.mpiniarski.distributedmonitor

import junit.framework.Assert.assertEquals
import mu.KotlinLogging
import org.junit.Test
import pl.mpiniarski.distributedmonitor.communication.Messenger
import pl.mpiniarski.distributedmonitor.communication.ZeroMqBinaryMessenger
import java.util.*
import kotlin.concurrent.thread

class DistributedMonitorTest {

    companion object {
        const val MAXCOUNT = 1
    }

    @Test
    fun producerConsumerTest() {

        class Buffer(private val size : Int, messenger : Messenger)
            : DistributedMonitor("buffer", messenger) {
            private val logger = KotlinLogging.logger { }

            private val empty = createCondition("empty")
            private val full = createCondition("full")
            private var values : Stack<Int> = Stack()

            fun produce(value : Int) = entry {
                if (values.size == size) {
                    full.await()
                }
                values.push(value)
                if (values.size == 1) {
                    empty.signal()
                }
            }

            fun consume() : Int = entry {
                if (values.size == 0) {
                    empty.await()
                }
                val value = values.pop()
                if (values.size == size - 1) {
                    full.signal()
                }
                return value
            }

            override fun serializeState() : ByteArray {
                return values.joinToString(",").toByteArray()
            }

            override fun deserializeAndUpdateState(state : ByteArray) {
                val stack = Stack<Int>()
                val string = String(state)
                if(!string.isEmpty()){
                    stack.addAll(string.split(',').map { it.toInt() }.toList())
                }
                values = stack
            }

        }


        val nodes = listOf(
                "tcp://localhost:5557",
                "tcp://localhost:5558"
        )

        val itemsRange = 1 .. 999

        val producer = thread(start = true) {
            val binaryMessenger = ZeroMqBinaryMessenger(nodes[0], nodes - nodes[0])
            val messenger = Messenger(binaryMessenger)
            val buffer = Buffer(MAXCOUNT, messenger)
            messenger.start()

            for (i in itemsRange) {
                Thread.sleep(Random().nextInt(100).toLong())
                buffer.produce(i)
            }
        }


        val consumer = thread(start = true) {
            val binaryMessenger = ZeroMqBinaryMessenger(nodes[1], nodes - nodes[1])
            val messenger = Messenger(binaryMessenger)
            val buffer = Buffer(MAXCOUNT, messenger)
            val result = ArrayList<Int>()

            messenger.start()

            for (i in itemsRange) {
                Thread.sleep(Random().nextInt(100).toLong())
                result.add(buffer.consume())
            }

            assertEquals(itemsRange.toList(), result)
        }

        producer.join()
        consumer.join()

    }
}