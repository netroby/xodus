package jetbrains.exodus.env

import jetbrains.exodus.core.dataStructures.hash.HashSet

internal class TransactionSet {

    private val txns = HashSet<Transaction>()
    private var minAddress: Long = Long.MAX_VALUE
    private var maxAddress: Long = Long.MIN_VALUE

    fun forEach(executable: TransactionalExecutable) {
        synchronized(txns) { txns.toArray(arrayOfNulls<Transaction>(txns.size)) }.forEach { txn ->
            executable.execute(txn!!)
        }
    }

    fun add(txn: Transaction, processMinLambda: (Long) -> Unit): Long {
        val highAddress = txn.highAddress
        return synchronized(txns) {
            txns.add(txn)
            if (maxAddress < highAddress) {
                maxAddress = highAddress
            }
            if (minAddress > highAddress) {
                minAddress = highAddress
                processMinLambda(highAddress)
            }
            minAddress
        }
    }

    fun remove(txn: Transaction, processMinLambda: (Long) -> Unit): Long {
        return synchronized(txns) {
            txns.remove(txn)
            var minAddress = Long.MAX_VALUE
            var maxAddress = Long.MIN_VALUE
            txns.forEach { txn ->
                val highAddress = txn.highAddress
                if (maxAddress < highAddress) {
                    maxAddress = highAddress
                }
                if (minAddress > highAddress) {
                    minAddress = highAddress
                }
            }
            if (this.minAddress != minAddress) {
                this.minAddress = minAddress
                processMinLambda(minAddress)
            }
            this.maxAddress = maxAddress
            minAddress
        }
    }

    fun isEmpty() = synchronized(txns) { txns.isEmpty() }

    fun size() = synchronized(txns) { txns.size }

    fun contains(txn: Transaction) = synchronized(txns) { txns.contains(txn) }

    val newestTxnHighAddress = synchronized(txns) { maxAddress }
}