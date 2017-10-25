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

import jetbrains.exodus.ExodusException
import org.agrona.concurrent.UnsafeBuffer
import java.io.File
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileLock
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

private const val FILE_NAME = "xd.lck"
private const val VERSION = 1
private const val UNUSED = -1L

// The offsets should be 4-byte aligned for volatile reads/writes
private const val VERSION_SIZE = 4
private const val WRITER_LOCK_OFFSET = VERSION_SIZE
private const val WRITER_LOCK_SIZE = 4
private const val HIGHEST_ROOT_OFFSET = WRITER_LOCK_OFFSET + WRITER_LOCK_SIZE
private const val HIGHEST_ROOT_SIZE = 8
private const val LOWEST_USED_ROOT_OFFSET = HIGHEST_ROOT_OFFSET + HIGHEST_ROOT_SIZE
private const val LOWEST_USED_ROOT_SIZE = 8
private const val RESERVED_SLOT_BITSET_OFFSET = LOWEST_USED_ROOT_OFFSET + LOWEST_USED_ROOT_SIZE
private const val RESERVED_SLOT_BITSET_SIZE = 8
private const val SLOTS_OFFSET = RESERVED_SLOT_BITSET_OFFSET + RESERVED_SLOT_BITSET_SIZE
private const val NUM_SLOTS = 64
private const val SLOT_SIZE = 8
private const val SLOTS_TOTAL_SIZE = NUM_SLOTS * SLOT_SIZE
private const val FILE_SIZE = SLOTS_OFFSET + SLOTS_TOTAL_SIZE

class FileBasedProcessCoordinator private constructor(
        private val file: CoordinationFile,
        private val slotIndex: Int
) : ProcessCoordinator {

    override fun tryAcquireWriterLock() = file.writerLock.tryAcquire()

    override var highestRoot: Long?
        get() = file.highestRoot.takeUnless { it == UNUSED }
        set(value) {
            require(value == null || value >= 0) { "The highest root should not be negative" }
            file.highestRootLock.withLock {
                require((value ?: UNUSED) >= file.highestRoot) {
                    "The new highest root should not be less than the previous"
                }
                file.highestRoot = value ?: UNUSED
            }
        }

    override val lowestUsedRoot: Long? get() = file.lowestUsedRoot

    override var localLowestUsedRoot: Long?
        get() = file.getSlotLowestUsedRoot(slotIndex)
        set(value) {
            require(value == null || value >= 0) { "The local lowest root should not be negative" }
            file.lowestUsedRootAndReservedSlotBitsetLock.withLock {
                if (value != localLowestUsedRoot) {
                    file.highestRootLock.withLock(optional = value == null || lowestUsedRoot != null) {
                        validateNewLocalLowestUsedRoot(value)
                        file.setSlotLowestUsedRoot(slotIndex, value)
                        file.recalculateLowestUsedRoot()
                    }
                }
            }
        }

    override fun <T> withHighestRootLock(action: () -> T) = file.highestRootLock.withLock { action() }

    private fun validateNewLocalLowestUsedRoot(newLocalLowestRoot: Long?) {
        file.lowestUsedRootAndReservedSlotBitsetLock.withLock {
            newLocalLowestRoot?.let {
                val highestRoot = file.highestRoot
                require(it <= highestRoot) {
                    "The local lowest root should not be greater than the highest root"
                }
                // Anything lower than that might have been garbage collected already
                require(it >= lowestUsedRoot ?: highestRoot) {
                    "The local lowest root should not be less than the global lowest root"
                }
                // Just a sanity check
                require(it >= localLowestUsedRoot ?: 0) {
                    "The new lowest local root should not be less than the previous"
                }
            }
        }
    }

    override fun withExclusiveLock(action: () -> Unit) = file.lowestUsedRootAndReservedSlotBitsetLock.withLock {
        file.refreshReservedSlotBitmask()
        if (file.isSlotReservedExclusively(slotIndex)) {
            action()
            true
        } else {
            false
        }
    }

    override fun close() {
        file.lowestUsedRootAndReservedSlotBitsetLock.withLock {
            file.reservedSlotBitset = file.reservedSlotBitset and getSlotBit(slotIndex).inv()
            file.recalculateLowestUsedRoot()
        }
        file.close()
    }

    companion object {
        fun create(databaseLocation: File): FileBasedProcessCoordinator {
            if (!databaseLocation.exists()) {
                if (!databaseLocation.mkdirs()) {
                    throw ExodusException("Cannot create database directory: " + databaseLocation)
                }
            }
            val file = RandomAccessFile(File(databaseLocation, FILE_NAME), "rw")

            return file.lockVersion().use {
                file.tryLockEverythingExceptVersion()?.use {
                    file.formatCoordinationFile()
                }

                file.checkCoordinationFileFormat()

                val coordinationFile = CoordinationFile(file)

                FileBasedProcessCoordinator(coordinationFile, slotIndex = coordinationFile.reserveSlot())
            }
        }
    }
}

private val SLOTS = 0 until NUM_SLOTS

private fun getSlotOffset(slotIndex: Int) = SLOTS_OFFSET + slotIndex * SLOT_SIZE

private fun getSlotBit(slotIndex: Int) = 1L shl slotIndex

// TODO: add timeout
private fun RandomAccessFile.lockVersion() = channel.lock(0L, VERSION_SIZE.toLong(), false)

private fun RandomAccessFile.tryLockEverythingExceptVersion() =
        channel.tryLock(VERSION_SIZE.toLong(), Long.MAX_VALUE - VERSION_SIZE, false)

private fun RandomAccessFile.formatCoordinationFile() {
    val buffer = ByteBuffer.allocate(SLOTS_OFFSET)

    buffer.putInt(VERSION)
    buffer.put(0) // writer lock
    buffer.putLong(UNUSED) // highest root
    buffer.putLong(UNUSED) // lowest used root
    buffer.putLong(0) // reserved slot bitset

    setLength(FILE_SIZE.toLong())
    seek(0)
    write(buffer.array(), buffer.arrayOffset(), SLOTS_OFFSET)
}

private fun RandomAccessFile.checkCoordinationFileFormat() {
    if (length() < VERSION_SIZE) {
        throw ExodusException("Bad coordination file")
    }
    seek(0L)
    if (readInt() != VERSION) {
        throw ExodusException("Incompatible version of coordination file")
    }
    if (length() != FILE_SIZE.toLong()) {
        throw ExodusException("Bad coordination file")
    }
}

private class CoordinationFile(private val file: RandomAccessFile) : AutoCloseable {
    private val map = UnsafeBuffer(file.channel.map(FileChannel.MapMode.READ_WRITE, 0, FILE_SIZE.toLong()))
    val writerLock = ReentrantFileLock(WRITER_LOCK_OFFSET, WRITER_LOCK_SIZE)
    val highestRootLock = ReentrantFileLock(HIGHEST_ROOT_OFFSET, HIGHEST_ROOT_SIZE)
    val lowestUsedRootAndReservedSlotBitsetLock =
            ReentrantFileLock(LOWEST_USED_ROOT_OFFSET, LOWEST_USED_ROOT_SIZE + RESERVED_SLOT_BITSET_SIZE)

    var highestRoot: Long
        inline get() = highestRootLock.withLock { map.getLongVolatile(HIGHEST_ROOT_OFFSET) }
        inline set(value) = highestRootLock.withLock { map.putLongVolatile(HIGHEST_ROOT_OFFSET, value) }

    var lowestUsedRoot: Long?
        inline get() = lowestUsedRootAndReservedSlotBitsetLock.withLock {
            map.getLongVolatile(LOWEST_USED_ROOT_OFFSET).takeUnless { it == UNUSED }
        }
        inline set(value) = lowestUsedRootAndReservedSlotBitsetLock.withLock {
            map.putLongVolatile(LOWEST_USED_ROOT_OFFSET, value ?: UNUSED)
        }

    var reservedSlotBitset: Long
        inline get() = lowestUsedRootAndReservedSlotBitsetLock.withLock {
            map.getLongVolatile(RESERVED_SLOT_BITSET_OFFSET)
        }
        inline set(value) = lowestUsedRootAndReservedSlotBitsetLock.withLock {
            map.putLongVolatile(RESERVED_SLOT_BITSET_OFFSET, value)
        }

    fun getSlotLowestUsedRoot(slotIndex: Int) = lowestUsedRootAndReservedSlotBitsetLock.withLock {
        map.getLongVolatile(getSlotOffset(slotIndex)).takeUnless { it == UNUSED }
    }

    fun setSlotLowestUsedRoot(slotIndex: Int, value: Long?): Unit = lowestUsedRootAndReservedSlotBitsetLock.withLock {
        map.putLongVolatile(getSlotOffset(slotIndex), value ?: UNUSED)
    }

    private fun getActuallyReservedSlots() = SLOTS.fold(0L) { bitset, slotIndex ->
        bitset or (file.channel.tryLock(getSlotOffset(slotIndex).toLong(), SLOT_SIZE.toLong(), false)?.use {
            getSlotBit(slotIndex)
        } ?: if (isSlotReserved(slotIndex)) 0L else throw ExodusException("Unreserved slot area is locked in the file"))
    }.inv()

    fun refreshReservedSlotBitmask() = lowestUsedRootAndReservedSlotBitsetLock.withLock {
        reservedSlotBitset = getActuallyReservedSlots()
        recalculateLowestUsedRoot()
    }

    fun reserveSlot(): Int = lowestUsedRootAndReservedSlotBitsetLock.withLock {
        refreshReservedSlotBitmask()

        val slotIndex = java.lang.Long.numberOfTrailingZeros(reservedSlotBitset.inv())
        if (slotIndex > NUM_SLOTS) {
            throw ExodusException("No free slots in the coordination file")
        }

        file.channel.tryLock(getSlotOffset(slotIndex).toLong(), SLOT_SIZE.toLong(), false)
                ?: throw ExodusException("Cannot lock free slot")

        setSlotLowestUsedRoot(slotIndex, null)

        reservedSlotBitset = reservedSlotBitset or getSlotBit(slotIndex)

        return slotIndex
    }

    fun recalculateLowestUsedRoot() = lowestUsedRootAndReservedSlotBitsetLock.withLock {
        var lowestUsedRoot = Long.MAX_VALUE
        forEachReservedSlot { slotIndex ->
            val slotLowestUsedRoot = map.getLongVolatile(SLOTS_OFFSET + slotIndex * SLOT_SIZE)
            if (slotLowestUsedRoot != UNUSED) {
                lowestUsedRoot = lowestUsedRoot.coerceAtMost(slotLowestUsedRoot)
            }
        }
        this.lowestUsedRoot = lowestUsedRoot.takeUnless { it == Long.MAX_VALUE }
    }

    private fun isSlotReserved(slotIndex: Int) = lowestUsedRootAndReservedSlotBitsetLock.withLock {
        (reservedSlotBitset and getSlotBit(slotIndex)) != 0L
    }

    fun isSlotReservedExclusively(slotIndex: Int) = lowestUsedRootAndReservedSlotBitsetLock.withLock {
        reservedSlotBitset == getSlotBit(slotIndex)
    }

    private inline fun forEachReservedSlot(action: (Int) -> Unit) = lowestUsedRootAndReservedSlotBitsetLock.withLock {
        SLOTS.forEach { slotIndex ->
            if (isSlotReserved(slotIndex)) {
                action(slotIndex)
            }
        }
    }

    override fun close() = file.close()

    inner class ReentrantFileLock(val position: Int, val size: Int) {
        private val synchronizationLock = ReentrantLock()
        private var fileLock: FileLock? = null
        val isLocked get() = fileLock != null

        fun acquire() = synchronizationLock.withLock {
            if (fileLock == null) {
                fileLock = file.channel.lock(position.toLong(), size.toLong(), false)
                true
            } else {
                false
            }
        }

        fun tryAcquire() = synchronizationLock.withLock {
            if (fileLock == null) {
                fileLock = file.channel.tryLock(position.toLong(), size.toLong(), false)
            }
            fileLock != null
        }

        fun release() = synchronizationLock.withLock {
            fileLock?.let {
                it.release()
                fileLock = null
            }
        }

        inline fun <R> withLock(optional: Boolean = false, action: () -> R): R {
            val isAcquired = !optional && acquire()
            return try {
                action()
            } finally {
                if (isAcquired) {
                    release()
                }
            }
        }
    }
}
