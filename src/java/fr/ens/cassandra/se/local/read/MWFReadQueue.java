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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fr.ens.cassandra.se.local.LocalTask;
import fr.ens.cassandra.se.op.ReadOperation;
import fr.ens.cassandra.se.oracle.Oracle;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.MonotonicClock;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.utils.MonotonicClock.Global.preciseTime;

public class MWFReadQueue extends AbstractReadQueue<Runnable>
{
    private static final Logger logger = LoggerFactory.getLogger(MWFReadQueue.class);

    private static final MonotonicClock clock = preciseTime;

    private final ListItem head;

    public MWFReadQueue(Map<String, String> parameters)
    {
        super(parameters);

        this.head = ListItem.empty();
        this.head.next = ListItem.empty();
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
        head.lock();

        logger.info("Adding runnable to queue");

        try
        {
            ListItem item = ListItem.of(preciseTime.now(), runnable);

            item.next = head.next;
            head.next = item;

            return true;
        }
        finally
        {
            head.unlock();
        }
    }

    @Override
    public Runnable poll()
    {
        long now = clock.now();

        while (true)
        {
            TraversalResult result = optimisticMax(now);

            result.left.lock();

            try
            {
                result.right.lock();

                try
                {
                    if (result.validate())
                    {
                        if (result.right.runnable() == null)
                        {
                            logger.info("No runnable");
                            return null;
                        }

                        result.left.next = result.right.next;
                        result.right.marked = true;

                        return result.right.runnable();
                    }
                }
                finally
                {
                    result.right.unlock();
                }
            }
            finally
            {
                result.left.unlock();
            }
        }
    }

    @Override
    public Runnable peek()
    {
        long now = clock.now();

        while (true)
        {
            TraversalResult result = optimisticMax(now);

            result.left.lock();

            try
            {
                result.right.lock();

                try
                {
                    if (result.validate())
                    {
                        return result.right.runnable();
                    }
                }
                finally
                {
                    result.right.unlock();
                }
            }
            finally
            {
                result.left.unlock();
            }
        }
    }

    private TraversalResult optimisticMax(long now)
    {
        ListItem before = head;
        ListItem current = head.next;

        ListItem pred = before;
        ListItem item = current;

        double max = 0;

        while (current.next != null)
        {
            Runnable runnable = current.runnable();

            if (runnable instanceof LocalTask.ReadTask)
            {
                ReadOperation op = ((LocalTask.ReadTask) runnable).getReadOperation();

                if (op != null && op.key() != null)
                {
                    Oracle<String, Integer> oracle = DatabaseDescriptor.getOracle("size");

                    int size = oracle.get(op.key());

                    if (size > 0)
                    {
                        double score = ((double) current.delay(now)) / size;

                        logger.info("Score for {} is currently {}", op.key(), score);

                        if (score > max)
                        {
                            max = score;
                            pred = before;
                            item = current;
                        }
                    }
                }
            }

            before = current;
            current = current.next;
        }

        return new TraversalResult(pred, item);
    }

    private static class TraversalResult extends Pair<ListItem, ListItem>
    {
        public TraversalResult(ListItem left, ListItem right)
        {
            super(left, right);
        }

        public boolean validate()
        {
            return !left.marked && !right.marked && left.next == right.next;
        }
    }

    private static class ListItem
    {
        private final Lock lock = new ReentrantLock();

        private final long timestamp;

        private final Runnable runnable;

        public ListItem next = null;

        public boolean marked = false;

        public ListItem(long timestamp, Runnable runnable)
        {
            this.timestamp = timestamp;
            this.runnable = runnable;
        }

        public static ListItem of(long timestamp, Runnable runnable)
        {
            return new ListItem(timestamp, runnable);
        }

        public static ListItem empty()
        {
            return of(0, null);
        }

        public long timestamp()
        {
            return timestamp;
        }

        public Runnable runnable()
        {
            return runnable;
        }

        public long delay(long now)
        {
            return now - timestamp;
        }

        public void lock()
        {
            lock.lock();
        }

        public void unlock()
        {
            lock.unlock();
        }
    }
}
