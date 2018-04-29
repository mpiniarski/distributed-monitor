package pl.mpiniarski.distributedmonitor

import pl.mpiniarski.distributedmonitor.communication.Messenger

open class DistributedMonitor(name : String, messenger : Messenger) {
    protected val lock : DistributedLock = DistributedLock("$name/lock", messenger)

    protected fun createCondition(name : String) : Condition {
        return lock.newCondition(name)
    }

    protected inline fun <T> entry(f : () -> T) : T {
        try {
            lock.lock()
            return f()
        } finally {
            lock.unlock()
        }
    }
}

