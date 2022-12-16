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

package fr.ens.cassandra.se.state.facts;

import fr.ens.cassandra.se.state.facts.impl.ByteArrayAggregator;
import fr.ens.cassandra.se.state.facts.impl.ByteArrayMeasure;
import fr.ens.cassandra.se.state.facts.impl.ByteArraySerializer;
import fr.ens.cassandra.se.state.facts.impl.CompletedReadAggregator;
import fr.ens.cassandra.se.state.facts.impl.CompletedReadMeasure;
import fr.ens.cassandra.se.state.facts.impl.CompletedReadSerializer;
import fr.ens.cassandra.se.state.facts.impl.PendingReadAggregator;
import fr.ens.cassandra.se.state.facts.impl.PendingReadMeasure;
import fr.ens.cassandra.se.state.facts.impl.PendingReadSerializer;
import fr.ens.cassandra.se.state.facts.impl.ServiceRateAggregator;
import fr.ens.cassandra.se.state.facts.impl.ServiceRateMeasure;
import fr.ens.cassandra.se.state.facts.impl.ServiceRateSerializer;

public enum Fact
{
    COMPLETED_READS(1, new CompletedReadSerializer(), new CompletedReadMeasure(), new CompletedReadAggregator()),
    BYTE_ARRAY_4(2, new ByteArraySerializer(4), new ByteArrayMeasure(4), new ByteArrayAggregator(4)),
    BYTE_ARRAY_16(3, new ByteArraySerializer(16), new ByteArrayMeasure(16), new ByteArrayAggregator(16)),
    BYTE_ARRAY_64(4, new ByteArraySerializer(64), new ByteArrayMeasure(64), new ByteArrayAggregator(64)),
    BYTE_ARRAY_256(5, new ByteArraySerializer(256), new ByteArrayMeasure(256), new ByteArrayAggregator(256)),
    BYTE_ARRAY_1024(6, new ByteArraySerializer(1024), new ByteArrayMeasure(1024), new ByteArrayAggregator(1024)),

    PENDING_READS(10, new PendingReadSerializer(), new PendingReadMeasure(), new PendingReadAggregator()),
    SERVICE_RATE(11, new ServiceRateSerializer(), new ServiceRateMeasure(), new ServiceRateAggregator());

    private final int id;
    private final FactSerializer serializer;
    private final FactMeasure measure;
    private final FactAggregator aggregator;

    private static final Fact[] ID_TO_FACT;

    static
    {
        Fact[] facts = values();

        int max = -1;
        for (Fact fact : facts)
        {
            max = Math.max(max, fact.id);
        }

        Fact[] map = new Fact[max + 1];

        for (Fact fact : facts)
        {
            if (map[fact.id] != null)
            {
                throw new IllegalArgumentException("Cannot have two facts that map to the same id: " + fact + " and " + map[fact.id]);
            }

            map[fact.id] = fact;
        }

        ID_TO_FACT = map;
    }

    Fact(int id, FactSerializer serializer, FactMeasure measure, FactAggregator aggregator)
    {
        this.id = id;
        this.serializer = serializer;
        this.measure = measure;
        this.aggregator = aggregator;
    }

    public static Fact fromId(int id)
    {
        Fact fact = id >= 0 && id < ID_TO_FACT.length ? ID_TO_FACT[id] : null;

        if (fact == null)
        {
            throw new IllegalArgumentException("Unknown fact id: " + id);
        }

        return fact;
    }

    public int getId()
    {
        return id;
    }

    public <T> FactSerializer<T> serializer()
    {
        return (FactSerializer<T>) serializer;
    }

    public <T> FactMeasure<T> measure()
    {
        return (FactMeasure<T>) measure;
    }

    public <T, U> FactAggregator<T, U> aggregator()
    {
        return (FactAggregator<T, U>) aggregator;
    }
}
