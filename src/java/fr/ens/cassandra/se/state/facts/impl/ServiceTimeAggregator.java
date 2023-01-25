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

package fr.ens.cassandra.se.state.facts.impl;

import java.util.concurrent.TimeUnit;

import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Snapshot;
import fr.ens.cassandra.se.state.StateFeedback;
import fr.ens.cassandra.se.state.facts.FactAggregator;

public class ServiceTimeAggregator implements FactAggregator<ServiceTimeAggregator.ServiceTime, Long>
{
    @Override
    public ServiceTime get()
    {
        return new ServiceTime();
    }

    @Override
    public ServiceTime apply(ServiceTime current, Long value, StateFeedback feedback)
    {
        current.update(value, feedback.timestamp());

        return current;
    }

    public static class ServiceTime
    {
        private static final double ALPHA = 0.75;
        private static final int WINDOW_SIZE = 100;

        private final ExponentiallyDecayingReservoir reservoir;

        private long lastValue = 0;
        private long lastTimestamp = 0;

        public ServiceTime()
        {
            this.reservoir = new ExponentiallyDecayingReservoir(WINDOW_SIZE, ALPHA);
        }

        public void update(long value, long timestamp)
        {
            double diff = value - lastValue;
            double delay = TimeUnit.NANOSECONDS.toMillis(timestamp - lastTimestamp);

            double rate = diff / delay;
            double time = rate > 0.0 ? 1 / rate : 0.0;

            reservoir.update((long) time);

            lastValue = value;
            lastTimestamp = timestamp;
        }

        public Snapshot getSnapshot()
        {
            return reservoir.getSnapshot();
        }
    }
}
