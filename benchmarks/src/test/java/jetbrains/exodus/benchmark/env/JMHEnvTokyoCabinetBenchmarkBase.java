/**
 * Copyright 2010 - 2016 JetBrains s.r.o.
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
package jetbrains.exodus.benchmark.env;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.benchmark.TokyoCabinetBenchmark;
import jetbrains.exodus.env.*;
import jetbrains.exodus.io.FileDataReader;
import jetbrains.exodus.io.FileDataWriter;
import jetbrains.exodus.log.Log;
import jetbrains.exodus.log.LogConfig;
import org.jetbrains.annotations.NotNull;
import org.junit.rules.TemporaryFolder;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;

import java.io.File;
import java.io.IOException;

abstract class JMHEnvTokyoCabinetBenchmarkBase {

    private static final ByteIterable[] successiveKeys = TokyoCabinetBenchmark.getSuccessiveEntries(TokyoCabinetBenchmark.KEYS_COUNT);
    static final ByteIterable[] randomKeys = TokyoCabinetBenchmark.getRandomEntries(TokyoCabinetBenchmark.KEYS_COUNT);

    Environment env;
    Store store;

    @Setup(Level.Invocation)
    public void setup() throws IOException {
        Log.invalidateSharedCache();
        TokyoCabinetBenchmark.shuffleKeys(randomKeys);
        final TemporaryFolder temporaryFolder = new TemporaryFolder();
        temporaryFolder.create();
        final File testsDirectory = temporaryFolder.newFolder("data");
        env = Environments.newInstance(LogConfig.create(new FileDataReader(testsDirectory, 16), new FileDataWriter(testsDirectory)));
        store = env.computeInTransaction(new TransactionalComputable<Store>() {
            @Override
            public Store compute(@NotNull Transaction txn) {
                return env.openStore("TokyoCabinetBenchmarkStore", getConfig(), txn);
            }
        });
    }

    @TearDown(Level.Invocation)
    public void tearDown() throws IOException {
        env.close();
    }

    void writeSuccessiveKeys() {
        env.executeInTransaction(new TransactionalExecutable() {
            @Override
            public void execute(@NotNull final Transaction txn) {
                for (final ByteIterable key : successiveKeys) {
                    store.add(txn, key, key);
                }
            }
        });
    }

    protected abstract StoreConfig getConfig();
}
