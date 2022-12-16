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

import java.io.IOException;
import java.util.Map;

import javax.annotation.Nullable;

import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fr.ens.cassandra.se.state.facts.Fact;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

import static org.apache.cassandra.utils.MonotonicClock.Global.preciseTime;

public class StateFeedback implements Comparable<StateFeedback>
{
    private static final Logger logger = LoggerFactory.getLogger(StateFeedback.class);

    public static final IVersionedSerializer<StateFeedback> serializer = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(StateFeedback feedback, DataOutputPlus out, int version) throws IOException
        {
            long payloadSize = 0;

            if (feedback != null)
            {
                payloadSize = feedback.payloadSize(version);
            }

            // Include payload size to make inference easier.
            // A payload size equal to 0 means no feedback included.
            out.writeUnsignedVInt(payloadSize);

            if (feedback != null)
            {
                long timestamp = feedback.timestamp();
                out.writeUnsignedVInt(timestamp);

                int count = feedback.size();
                out.writeUnsignedVInt(count);

                for (Map.Entry<Fact, Object> entry : feedback.values().entrySet())
                {
                    Fact fact = entry.getKey();
                    out.writeUnsignedVInt(fact.getId());

                    Object value = entry.getValue();
                    fact.serializer().serialize(value, out, version);
                }
            }
        }

        @Override
        public StateFeedback deserialize(DataInputPlus in, int version) throws IOException
        {
            long payloadSize = in.readUnsignedVInt();

            if (payloadSize <= 0)
            {
                return null;
            }

            long timestamp = in.readUnsignedVInt();
            int count = (int) in.readUnsignedVInt();

            StateFeedback feedback = new StateFeedback(timestamp);

            for (int i = 0; i < count; ++i)
            {
                int id = (int) in.readUnsignedVInt();

                Fact fact = Fact.fromId(id);
                Object value = fact.serializer().deserialize(in, version);

                feedback.put(fact, value);
            }

            return feedback;
        }

        @Override
        public long serializedSize(StateFeedback feedback, int version)
        {
            long payloadSize = 0;

            if (feedback != null)
            {
                payloadSize = feedback.payloadSize(version);
            }

            return TypeSizes.sizeofUnsignedVInt(payloadSize) + payloadSize;
        }
    };

    private final long timestamp;

    private final Map<Fact, Object> values = Maps.newEnumMap(Fact.class);

    public StateFeedback()
    {
        this(preciseTime.now());
    }

    public StateFeedback(long timestamp)
    {
        this.timestamp = timestamp;
    }

    public long timestamp()
    {
        return timestamp;
    }

    public Map<Fact, Object> values()
    {
        return values;
    }

    public int size()
    {
        return values.size();
    }

    public StateFeedback put(Fact fact, Object value)
    {
        values.put(fact, value);

        return this;
    }

    public StateFeedback put(Iterable<Fact> facts)
    {
        for (Fact fact : facts)
        {
            put(fact, fact.measure().get());
        }

        return this;
    }

    public long payloadSize(int version)
    {
        long size = 0;
        size += TypeSizes.sizeofUnsignedVInt(timestamp);
        size += TypeSizes.sizeofUnsignedVInt(size());

        for (Map.Entry<Fact, Object> entry : values.entrySet())
        {
            Fact fact = entry.getKey();
            Object value = entry.getValue();

            size += TypeSizes.sizeofUnsignedVInt(fact.getId());
            size += fact.serializer().serializedSize(value, version);
        }

        return size;
    }

    @Override
    public int compareTo(StateFeedback feedback)
    {
        return Long.compare(timestamp, feedback.timestamp());
    }

    @Override
    public String toString()
    {
        return "state(" + values + ") [" + timestamp + ']';
    }
}
