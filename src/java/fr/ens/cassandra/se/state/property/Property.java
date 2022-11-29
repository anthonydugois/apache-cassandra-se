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

package fr.ens.cassandra.se.state.property;

import java.io.IOException;
import java.util.List;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public enum Property
{
    COMPLETED_READS(1,
                    new PropertySerializer<Long>()
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
                    new PropertyMeasure<Long>()
                    {
                        @Override
                        public Long get()
                        {
                            return Stage.READ.executor().getCompletedTaskCount();
                        }
                    },
                    new PropertyAggregator<Long, Long>()
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
    private final PropertySerializer serializer;
    private final PropertyMeasure measure;
    private final PropertyAggregator aggregator;

    private static final List<Property> properties = List.of(COMPLETED_READS);

    private static final Property[] idToProperty;

    static
    {
        Property[] properties = values();

        int max = -1;
        for (Property property : properties)
        {
            max = Math.max(max, property.id);
        }

        Property[] idMap = new Property[max + 1];

        for (Property property : properties)
        {
            if (idMap[property.id] != null)
            {
                throw new IllegalArgumentException("Cannot have two props that map to the same id: " + property + " and " + idMap[property.id]);
            }

            idMap[property.id] = property;
        }

        idToProperty = idMap;
    }

    Property(int id, PropertySerializer serializer, PropertyMeasure measure, PropertyAggregator aggregator)
    {
        this.id = id;
        this.serializer = serializer;
        this.measure = measure;
        this.aggregator = aggregator;
    }

    public static List<Property> properties()
    {
        return properties;
    }

    public static Property fromId(int id)
    {
        Property property = id >= 0 && id < idToProperty.length ? idToProperty[id] : null;

        if (property == null)
        {
            throw new IllegalArgumentException("Unknown property id: " + id);
        }

        return property;
    }

    public int getId()
    {
        return id;
    }

    public <T> PropertySerializer<T> serializer()
    {
        return (PropertySerializer<T>) serializer;
    }

    public <T> PropertyMeasure<T> measure()
    {
        return (PropertyMeasure<T>) measure;
    }

    public <T, U> PropertyAggregator<T, U> aggregator()
    {
        return (PropertyAggregator<T, U>) aggregator;
    }
}
