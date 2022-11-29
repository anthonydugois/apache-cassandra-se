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

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fr.ens.cassandra.se.state.property.Property;
import org.apache.cassandra.locator.InetAddressAndPort;

public class EndpointState implements Runnable
{
    private static final Logger logger = LoggerFactory.getLogger(EndpointState.class);

    private final BlockingQueue<StateFeedback> queue = new PriorityBlockingQueue<>();

    private final Map<Property, Object> values = new EnumMap<>(Property.class);

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

    public Object get(Property property)
    {
        Object value = values.get(property);

        if (value == null)
        {
            return property.aggregator().get();
        }

        return value;
    }

    public EndpointState add(StateFeedback feedback)
    {
        if (feedback.size() > 0)
        {
            try
            {
                queue.put(feedback);
            }
            catch (InterruptedException exception)
            {
                throw new RuntimeException(exception);
            }
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
                    for (Map.Entry<Property, Object> entry : feedback.values().entrySet())
                    {
                        Property property = entry.getKey();
                        Object value = entry.getValue();
                        Object current = get(property);

                        values.put(property, property.aggregator().apply(current, value));
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
