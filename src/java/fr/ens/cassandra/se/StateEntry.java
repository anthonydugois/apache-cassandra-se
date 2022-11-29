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

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public enum StateEntry
{
    COMPLETED_READS(0, true, new StateProperty<Long>()
    {
        @Override
        public Long produce()
        {
            return Stage.READ.executor().getCompletedTaskCount();
        }

        @Override
        public IVersionedSerializer<Long> serializer()
        {
            return new IVersionedSerializer<>()
            {
                @Override
                public void serialize(Long value, DataOutputPlus out, int version) throws IOException
                {
                    out.writeUnsignedVInt(value);
                }

                @Override
                public Long deserialize(DataInputPlus in, int version) throws IOException
                {
                    return null;
                }

                @Override
                public long serializedSize(Long value, int version)
                {
                    return 0;
                }
            };
        }
    });

    private final int id;
    private final boolean active;
    private final StateProperty<?> property;

    StateEntry(int id, boolean active, StateProperty<?> property)
    {
        this.id = id;
        this.active = active;
        this.property = property;
    }

    public <T> T produce()
    {
        return (T) property.produce();
    }

    public <T> IVersionedSerializer<T> serializer()
    {
        return (IVersionedSerializer<T>) property.serializer();
    }

    public boolean isActive()
    {
        return active;
    }
}
