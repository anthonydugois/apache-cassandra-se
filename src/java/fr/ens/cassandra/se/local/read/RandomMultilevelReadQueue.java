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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fr.ens.cassandra.se.local.LocalTask;
import fr.ens.cassandra.se.op.ReadOperation;
import fr.ens.cassandra.se.op.info.Info;
import io.netty.util.internal.ThreadLocalRandom;

/**
 * This queue is composed of multiple queues, which are attached a weight. The first queue has the highest priority,
 * thus it is attached the highest weight. The second queue has the second highest weight, third queue the third highest
 * weight, etc.
 * <p>
 * When polling, we pick a random integer between 0 and the weight sum (uniformly). Then we check in which interval this
 * random value is falling; if it is between 0 and first weight, then we poll from the first queue, which has the
 * highest priority. If the random value falls between first weight and the sum of the two first weights, then we poll
 * from the second queue, etc.
 * <p>
 * For now, the number of queues is configurable, but the weights are fixed: 2^n, 2^(n-1), 2^(n-2), etc, where n is
 * total number of queues. With two queues, this means that first queue will be chosen with probability 2/3, whereas the
 * second queue will be chosen with probability 1/3.
 */
public class RandomMultilevelReadQueue extends AbstractReadQueue<Runnable>
{
    private static final Logger logger = LoggerFactory.getLogger(RandomMultilevelReadQueue.class);

    private static final String LEVELS_PROPERTY = "levels";
    private static final String DEFAULT_LEVELS_PROPERTY = "10";

    private final int levels;

    private final List<ConcurrentLinkedQueue<Runnable>> queues;

    private final List<Integer> weights;

    private final int totalWeight;

    public RandomMultilevelReadQueue(Map<String, String> parameters)
    {
        super(parameters);

        this.levels = Integer.parseInt(parameters.getOrDefault(LEVELS_PROPERTY, DEFAULT_LEVELS_PROPERTY));

        this.queues = Lists.newArrayListWithCapacity(this.levels);
        this.weights = Lists.newArrayListWithCapacity(this.levels);

        int totalWeight = 0;

        for (int i = 0; i < this.levels; ++i)
        {
            int weight = (int) Math.pow(2, this.levels - i);

            queues.add(Queues.newConcurrentLinkedQueue());
            weights.add(weight);

            totalWeight += weight;
        }

        this.totalWeight = totalWeight;
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
        // By default, pick a random queue.
        int index = ThreadLocalRandom.current().nextInt(levels);

        if (runnable instanceof LocalTask.ReadTask)
        {
            ReadOperation op = ((LocalTask.ReadTask) runnable).getReadOperation();

            if (op != null && op.has(Info.PRIORITY))
            {
                // If we got a read task, and it has a priority attached, pick the corresponding queue.
                index = (int) op.info(Info.PRIORITY);

                // Make sure the priority value does fit in the queue.
                index = Math.min(levels - 1, Math.max(0, index));
            }
        }

        return queues.get(index).offer(runnable);
    }

    @Override
    public Runnable poll()
    {
        while (true)
        {
            int choice = ThreadLocalRandom.current().nextInt(totalWeight);

            int acc = 0;

            for (int i = 0; i < levels; ++i)
            {
                acc += weights.get(i);

                if (acc > choice)
                {
                    Runnable runnable = queues.get(i).poll();

                    if (runnable != null)
                    {
                        return runnable;
                    }
                }
            }
        }
    }

    @Override
    public Runnable peek()
    {
        while (true)
        {
            int choice = ThreadLocalRandom.current().nextInt(totalWeight);

            int acc = 0;

            for (int i = 0; i < levels; ++i)
            {
                acc += weights.get(i);

                if (acc > choice)
                {
                    Runnable runnable = queues.get(i).peek();

                    if (runnable != null)
                    {
                        return runnable;
                    }
                }
            }
        }
    }
}
