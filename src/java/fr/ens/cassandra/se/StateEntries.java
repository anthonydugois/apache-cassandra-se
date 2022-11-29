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

package fr.ens.cassandra.se;

import java.io.IOException;
import java.util.EnumMap;
import java.util.Map;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.FBUtilities;

public class StateEntries implements Comparable<StateEntries>
{
    public static final IVersionedSerializer<StateEntries> serializer = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(StateEntries entries, DataOutputPlus out, int version) throws IOException
        {
            for (Map.Entry<StateEntry, Object> e : entries.getValues().entrySet())
            {
                StateEntry entry = e.getKey();
                Object value = e.getValue();

                entry.serializer().serialize(value, out, version);
            }
        }

        @Override
        public StateEntries deserialize(DataInputPlus in, int version) throws IOException
        {
            return null;
        }

        @Override
        public long serializedSize(StateEntries entries, int version)
        {
            return 0;
        }
    };

    private final InetAddressAndPort address;
    private final long timestamp;
    private final Map<StateEntry, Object> values;

    public StateEntries(InetAddressAndPort address, long timestamp, Map<StateEntry, Object> values)
    {
        this.address = address;
        this.timestamp = timestamp;
        this.values = values;
    }

    public static StateEntries create()
    {
        return create(FBUtilities.getLocalAddressAndPort(), System.nanoTime());
    }

    public static StateEntries create(InetAddressAndPort from, long timestamp)
    {
        EnumMap<StateEntry, Object> values = new EnumMap<>(StateEntry.class);

        for (StateEntry entry : StateEntry.values())
        {
            if (entry.isActive())
            {
                values.put(entry, entry.produce());
            }
        }

        return new StateEntries(from, timestamp, values);
    }

    public InetAddressAndPort getAddress()
    {
        return address;
    }

    public long getTimestamp()
    {
        return timestamp;
    }

    public Map<StateEntry, Object> getValues()
    {
        return values;
    }

    @Override
    public int compareTo(StateEntries feedback)
    {
        return Long.compare(timestamp, feedback.getTimestamp());
    }
}
