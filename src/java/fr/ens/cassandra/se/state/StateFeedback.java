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
import java.util.EnumMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fr.ens.cassandra.se.state.property.Property;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class StateFeedback
{
    private static final Logger logger = LoggerFactory.getLogger(StateFeedback.class);

    public static final IVersionedSerializer<StateFeedback> serializer = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(StateFeedback feedback, DataOutputPlus out, int version) throws IOException
        {
            long payloadSize = feedback.payloadSize(version);
            int count = feedback.size();

            out.writeUnsignedVInt(payloadSize); // include payload size to make inference easier
            out.writeUnsignedVInt(count);

            for (Map.Entry<Property, Object> entry : feedback.values().entrySet())
            {
                Property property = entry.getKey();
                Object value = entry.getValue();

                out.writeUnsignedVInt(property.getId());
                property.serializer().serialize(value, out, version);
            }
        }

        @Override
        public StateFeedback deserialize(DataInputPlus in, int version) throws IOException
        {
            long payloadSize = in.readUnsignedVInt();
            int count = (int) in.readUnsignedVInt();

            StateFeedback feedback = new StateFeedback();

            for (int i = 0; i < count; ++i)
            {
                int id = (int) in.readUnsignedVInt();

                Property property = Property.fromId(id);
                Object value = property.serializer().deserialize(in, version);

                feedback.put(property, value);
            }

            return feedback;
        }

        @Override
        public long serializedSize(StateFeedback feedback, int version)
        {
            long payloadSize = feedback.payloadSize(version);

            return TypeSizes.sizeofUnsignedVInt(payloadSize) + payloadSize;
        }
    };

    private final Map<Property, Object> values = new EnumMap<>(Property.class);

    public Map<Property, Object> values()
    {
        return values;
    }

    public int size()
    {
        return values.size();
    }

    public StateFeedback put(Property property, Object value)
    {
        values.put(property, value);

        return this;
    }

    public StateFeedback put(Property... properties)
    {
        for (Property property : properties)
        {
            put(property, property.measure().get());
        }

        return this;
    }

    public long payloadSize(int version)
    {
        long size = 0;
        size += TypeSizes.sizeofUnsignedVInt(size());

        for (Map.Entry<Property, Object> entry : values.entrySet())
        {
            Property property = entry.getKey();
            Object value = entry.getValue();

            size += TypeSizes.sizeofUnsignedVInt(property.getId());
            size += property.serializer().serializedSize(value, version);
        }

        return size;
    }

    @Override
    public String toString()
    {
        return values.toString();
    }
}
