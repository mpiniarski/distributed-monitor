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
        val logger = KotlinLogging.logger { }
    }

    class Buffer(private val size : Int, val messenger : Messenger)
        : DistributedMonitor("buffer", messenger) {
        private val logger = KotlinLogging.logger { }

        private val empty = createCondition("empty")
        private val full = createCondition("full")
        private var values : Stack<Int> = Stack()

        fun produce(value : Int) = entry {
            while (values.size == size) {
                full.await()
            }
            values.push(value)
            if (values.size == 1) {
                empty.signal()
            }
            logger.debug { "\t${messenger.localNode}\t Produced $value" }
        }

        fun consume() : Int = entry {
            while (values.size == 0) {
                empty.await()
            }
            val value = values.pop()
            if (values.size == size - 1) {
                full.signal()
            }
            logger.debug { "\t${messenger.localNode}\t Consumed $value" }
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

    enum class Role {
        PRODUCER,
        CONSUMER
    }

    @Test
    fun producerConsumerTest() {

        val maxCount = 1

        val nodes = mapOf<String, Role>(
                "localhost:5550" to Role.PRODUCER,
                "localhost:5555" to Role.CONSUMER
        )

        val itemsRange = 1 .. 9

        nodes.map { Pair(ZeroMqBinaryMessenger(it.key, (nodes.keys - it.key).toList()), it.value) }
                .map { Pair(Messenger(it.first), it.second) }
                .map { Triple(Buffer(maxCount, it.first), it.first, it.second) }
                .map {
                    val buffer = it.first
                    val messenger = it.second
                    val role = it.third
                    val thread : Thread
                    when (role) {
                        Role.PRODUCER -> {
                            thread = thread(start = true) {
                                messenger.start()
                                for (i in itemsRange) {
                                    Thread.sleep(Random().nextInt(100).toLong())
                                    buffer.produce(i)
                                }
                            }
                        }
                        Role.CONSUMER -> {
                            thread = thread(start = true) {
                                val result = ArrayList<Int>()
                                messenger.start()
                                for (i in itemsRange) {
                                    Thread.sleep(Random().nextInt(100).toLong())
                                    val value = buffer.consume()
                                    result.add(value)
                                }

                                assertEquals(itemsRange.toList(), result)
                            }
                        }
                    }
                    Pair(thread, messenger)
                }.map {
                    it.first.join()
                    it.second
                }
                .map {
                    it.close()
                }
    }

    @Test
    fun multipleProducerConsumerTest() {

        val maxCount = 1

        val nodes = mapOf(
                "localhost:5550" to Role.PRODUCER,
                "localhost:5551" to Role.PRODUCER,
                "localhost:5560" to Role.CONSUMER,
                "localhost:5561" to Role.CONSUMER
        )

        val itemsRange = 1 .. 10000

        val result = ArrayList<Int>()

        nodes.map { Pair(ZeroMqBinaryMessenger(it.key, (nodes.keys - it.key).toList()), it.value) }
                .map { Pair(Messenger(it.first), it.second) }
                .map { Triple(Buffer(maxCount, it.first), it.first, it.second) }
                .map {
                    val buffer = it.first
                    val messenger = it.second
                    val role = it.third
                    val thread : Thread
                    when (role) {
                        Role.PRODUCER -> {
                            thread = thread(start = true) {
                                messenger.start()
                                for (i in itemsRange) {
                                    Thread.sleep(Random().nextInt(100).toLong())
                                    buffer.produce(i)
                                }
                                logger.debug { "\t${messenger.localNode}\tFINISHED" }
                            }
                        }
                        Role.CONSUMER -> {
                            thread = thread(start = true) {
                                messenger.start()
                                for (i in itemsRange) {
                                    Thread.sleep(Random().nextInt(100).toLong())
                                    val value = buffer.consume()
                                    result.add(value)
                                    logger.debug { "\t${messenger.localNode}\tConsumeNum $i" }
                                }
                                logger.debug { "\t${messenger.localNode}\tFINISHED" }
                            }
                        }
                    }
                    Pair(thread, messenger)
                }.map {
                    it.first.join()
                    it.second
                }
                .map {
                    it.close()
                }

        val items = itemsRange.toList()
        result.containsAll(items + items + items)
    }
}
