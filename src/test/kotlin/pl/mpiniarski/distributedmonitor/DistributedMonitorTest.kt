package pl.mpiniarski.distributedmonitor

import junit.framework.Assert.assertEquals
import mu.KotlinLogging
import org.junit.Test
import pl.mpiniarski.distributedmonitor.communication.StandardMessenger
import pl.mpiniarski.distributedmonitor.communication.ZeroMqBinaryMessenger
import java.util.*
import kotlin.concurrent.thread

class DistributedMonitorTest {

    companion object {
        const val MAXCOUNT = 1
    }

    @Test
    fun producerConsumerTest() {

        class Buffer(private val size : Int, messenger : StandardMessenger)
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
                if (!string.isEmpty()) {
                    stack.addAll(string.split(',').map { it.toInt() }.toList())
                }
                values = stack
            }

        }


        val nodes = listOf(
                "tcp://localhost:5557",
                "tcp://localhost:5558"
        )

        val itemsRange = 1 .. 9

        val producerBinaryMessenger = ZeroMqBinaryMessenger(nodes[0], nodes - nodes[0])
        val producerMessenger = StandardMessenger(producerBinaryMessenger)
        val producerBuffer = Buffer(MAXCOUNT, producerMessenger)
        val producer = thread(start = true) {
            producerMessenger.start()

            for (i in itemsRange) {
                Thread.sleep(Random().nextInt(100).toLong())
                producerBuffer.produce(i)
            }
        }


        val consumerBinaryMessenger = ZeroMqBinaryMessenger(nodes[1], nodes - nodes[1])
        val consumerMessenger = StandardMessenger(consumerBinaryMessenger)
        val consumerBuffer = Buffer(MAXCOUNT, consumerMessenger)
        val consumer = thread(start = true) {
            val result = ArrayList<Int>()

            consumerMessenger.start()

            for (i in itemsRange) {
                Thread.sleep(Random().nextInt(100).toLong())
                result.add(consumerBuffer.consume())
            }

            assertEquals(itemsRange.toList(), result)
        }

        producer.join()
        consumer.join()
        consumerBuffer.close()
        producerMessenger.close()

    }
}