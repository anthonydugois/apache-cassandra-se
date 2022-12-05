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

package fr.ens.cassandra.se.local.read;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fr.ens.cassandra.se.local.LocalTask;
import fr.ens.cassandra.se.op.ReadOperation;
import fr.ens.cassandra.se.op.info.Info;

public class MultilevelReadQueue extends AbstractReadQueue<Runnable>
{
    private static final Logger logger = LoggerFactory.getLogger(MultilevelReadQueue.class);

    public static final String LEVELS_PROPERTY = "levels";
    public static final String DEFAULT_LEVELS_PROPERTY = "10";

    private final int levels;

    private final List<ConcurrentLinkedQueue<Runnable>> queues;

    public MultilevelReadQueue(Map<String, String> parameters)
    {
        super(parameters);

        this.levels = Integer.parseInt(parameters.getOrDefault(LEVELS_PROPERTY, DEFAULT_LEVELS_PROPERTY));
        this.queues = new ArrayList<>(this.levels);

        for (int i = 0; i < this.levels; ++i)
        {
            ConcurrentLinkedQueue<Runnable> queue = new ConcurrentLinkedQueue<>();

            queues.set(i, queue);
        }
    }

    @Override
    public Iterator<Runnable> iterator()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int size()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean offer(Runnable runnable)
    {
        ConcurrentLinkedQueue<Runnable> queue = queues.get(levels - 1);

        if (runnable instanceof LocalTask.ReadTask)
        {
            ReadOperation op = ((LocalTask.ReadTask) runnable).getReadOperation();
            int priority = (int) op.info(Info.PRIORITY);

            queue = queues.get(priority);
        }

        return queue.offer(runnable);
    }

    @Override
    public Runnable poll()
    {
        for (ConcurrentLinkedQueue<Runnable> queue : queues)
        {
            if (!queue.isEmpty())
            {
                return queue.poll();
            }
        }

        return null;
    }

    @Override
    public Runnable peek()
    {
        for (ConcurrentLinkedQueue<Runnable> queue : queues)
        {
            if (!queue.isEmpty())
            {
                return queue.peek();
            }
        }

        return null;
    }
}
