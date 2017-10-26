/**
 * Copyright 2010 - 2017 JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

    val newestTxnHighAddress: Long get() = synchronized(txns) { maxAddress }
}