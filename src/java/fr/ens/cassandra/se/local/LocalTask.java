/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package fr.ens.cassandra.se.local;

import fr.ens.cassandra.se.op.ReadOperation;
import fr.ens.cassandra.se.op.ReadOperationProvider;
import org.apache.cassandra.db.ReadCommand;

public abstract class LocalTask implements Runnable
{
    private final Runnable runnable;

    public LocalTask(Runnable runnable)
    {
        this.runnable = runnable;
    }

    public static Runnable create(Runnable task, Runnable runnable)
    {
        if (task instanceof ReadOperationProvider)
        {
            return new ReadTask((ReadOperationProvider) task, runnable);
        }

        return runnable;
    }

    @Override
    public void run()
    {
        runnable.run();
    }

    public static class ReadTask extends LocalTask implements ReadOperationProvider
    {
        private final ReadOperationProvider provider;

        public ReadTask(ReadOperationProvider provider, Runnable runnable)
        {
            super(runnable);

            this.provider = provider;
        }

        @Override
        public <T extends ReadCommand> ReadOperation<T> getReadOperation()
        {
            return provider.getReadOperation();
        }
    }
}
