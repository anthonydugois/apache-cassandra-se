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

import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.PriorityBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fr.ens.cassandra.se.local.LocalTask;
import fr.ens.cassandra.se.op.ReadOperation;
import org.apache.cassandra.db.ReadCommand;

public class PriorityReadQueue extends AbstractReadQueue<Runnable>
{
    private static final Logger logger = LoggerFactory.getLogger(PriorityReadQueue.class);

    private final Queue<Ordered<Runnable>> queue = new PriorityBlockingQueue<>();

    public PriorityReadQueue(Map<String, String> parameters)
    {
        super(parameters);
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
        int priority = Integer.MAX_VALUE;

        if (runnable instanceof LocalTask.ReadTask)
        {
            ReadOperation op = ((LocalTask.ReadTask) runnable).getReadOperation();
            ReadCommand command = op.command();

            // retrieve priority data from command and insert it in queue
            // int priority = command.get("priority")
            priority = 0;
        }

        return queue.offer(new Ordered<>(priority, runnable));
    }

    @Override
    public Runnable poll()
    {
        Ordered<Runnable> ordered = queue.poll();

        if (ordered != null)
        {
            return ordered.getElement();
        }

        return null;
    }

    @Override
    public Runnable peek()
    {
        Ordered<Runnable> ordered = queue.peek();

        if (ordered != null)
        {
            return ordered.getElement();
        }

        return null;
    }

    public static class Ordered<T> implements Comparable<Ordered<T>>
    {
        private final int priority;
        private final T element;

        public Ordered(int priority, T element)
        {
            this.priority = priority;
            this.element = element;
        }

        public int getPriority()
        {
            return priority;
        }

        public T getElement()
        {
            return element;
        }

        @Override
        public int compareTo(Ordered<T> other)
        {
            return Integer.compare(priority, other.getPriority());
        }
    }
}
