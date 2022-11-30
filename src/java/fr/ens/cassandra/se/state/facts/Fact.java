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

import java.io.IOException;
import java.util.List;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public enum Fact
{
    COMPLETED_READS(1,
                    new FactSerializer<Long>()
                    {
                        @Override
                        public void serialize(Long value, DataOutputPlus out, int version) throws IOException
                        {
                            out.writeUnsignedVInt(value);
                        }

                        @Override
                        public Long deserialize(DataInputPlus in, int version) throws IOException
                        {
                            return in.readUnsignedVInt();
                        }

                        @Override
                        public long serializedSize(Long value, int version)
                        {
                            return TypeSizes.sizeofUnsignedVInt(value);
                        }
                    },
                    new FactMeasure<Long>()
                    {
                        @Override
                        public Long get()
                        {
                            return Stage.READ.executor().getCompletedTaskCount();
                        }
                    },
                    new FactAggregator<Long, Long>()
                    {
                        @Override
                        public Long get()
                        {
                            return 0L;
                        }

                        @Override
                        public Long apply(Long current, Long value)
                        {
                            return value;
                        }
                    });

    private final int id;
    private final FactSerializer serializer;
    private final FactMeasure measure;
    private final FactAggregator aggregator;

    private static final List<Fact> facts = List.of(COMPLETED_READS);

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

    public static List<Fact> facts()
    {
        return facts;
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
