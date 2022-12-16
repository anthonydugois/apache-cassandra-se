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

package fr.ens.cassandra.se.state;

import java.util.Map;
import java.util.concurrent.PriorityBlockingQueue;

import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fr.ens.cassandra.se.state.facts.Fact;
import org.apache.cassandra.locator.InetAddressAndPort;

public class EndpointState implements Runnable
{
    private static final Logger logger = LoggerFactory.getLogger(EndpointState.class);

    private final PriorityBlockingQueue<StateFeedback> queue = Queues.newPriorityBlockingQueue();

    private final Map<Fact, Object> values = Maps.newEnumMap(Fact.class);

    private final InetAddressAndPort address;

    private long lastTimestamp = 0;

    public EndpointState(InetAddressAndPort address)
    {
        this.address = address;
    }

    public InetAddressAndPort address()
    {
        return address;
    }

    public Object get(Fact fact)
    {
        Object value = values.get(fact);

        if (value == null)
        {
            return fact.aggregator().get();
        }

        return value;
    }

    public EndpointState add(StateFeedback feedback)
    {
        if (feedback != null && feedback.size() > 0)
        {
            queue.put(feedback);
        }

        return this;
    }

    @Override
    public void run()
    {
        try
        {
            while (true)
            {
                StateFeedback feedback = queue.take();

                if (feedback.timestamp() >= lastTimestamp)
                {
                    for (Map.Entry<Fact, Object> entry : feedback.values().entrySet())
                    {
                        Fact fact = entry.getKey();
                        Object value = entry.getValue();
                        Object current = get(fact);

                        values.put(fact, fact.aggregator().apply(current, value, feedback));
                    }

                    lastTimestamp = feedback.timestamp();
                }
            }
        }
        catch (InterruptedException exception)
        {
            throw new RuntimeException(exception);
        }
    }
}
