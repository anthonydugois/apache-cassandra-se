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

package fr.ens.cassandra.se.op;

import java.io.IOException;
import java.util.EnumMap;
import java.util.Map;

import fr.ens.cassandra.se.op.info.Info;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class ReadCommandInfos
{
    public static final IVersionedSerializer<ReadCommandInfos> serializer = new IVersionedSerializer<ReadCommandInfos>()
    {
        @Override
        public void serialize(ReadCommandInfos infos, DataOutputPlus out, int version) throws IOException
        {
            int count = infos.size();

            out.writeUnsignedVInt(count);

            for (Map.Entry<Info, Object> entry : infos.values().entrySet())
            {
                Info info = entry.getKey();
                Object value = entry.getValue();

                out.writeUnsignedVInt(info.id());
                info.serializer().serialize(value, out, version);
            }
        }

        @Override
        public ReadCommandInfos deserialize(DataInputPlus in, int version) throws IOException
        {
            int count = (int) in.readUnsignedVInt();

            ReadCommandInfos infos = new ReadCommandInfos();

            for (int i = 0; i < count; ++i)
            {
                int id = (int) in.readUnsignedVInt();

                Info info = Info.fromId(id);
                Object value = info.serializer().deserialize(in, version);

                infos.put(info, value);
            }

            return infos;
        }

        @Override
        public long serializedSize(ReadCommandInfos infos, int version)
        {
            long payloadSize = infos.payloadSize(version);

            return payloadSize;
        }
    };

    private final Map<Info, Object> values = new EnumMap<>(Info.class);

    public Map<Info, Object> values()
    {
        return values;
    }

    public int size()
    {
        return values.size();
    }

    public Object get(Info info)
    {
        return values.get(info);
    }

    public ReadCommandInfos put(Info info, Object value)
    {
        values.put(info, value);

        return this;
    }

    public long payloadSize(int version)
    {
        long size = 0;
        size += TypeSizes.sizeofUnsignedVInt(size());

        for (Map.Entry<Info, Object> entry : values.entrySet())
        {
            Info info = entry.getKey();
            Object value = entry.getValue();

            size += TypeSizes.sizeofUnsignedVInt(info.id());
            size += info.serializer().serializedSize(value, version);
        }

        return size;
    }
}
