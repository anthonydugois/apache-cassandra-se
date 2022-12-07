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

package fr.ens.cassandra.se.op.info;

import java.io.IOException;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public enum Info
{
    PRIORITY(1,
             new InfoSerializer<Integer>()
             {
                 @Override
                 public void serialize(Integer priority, DataOutputPlus out, int version) throws IOException
                 {
                     out.writeUnsignedVInt(priority);
                 }

                 @Override
                 public Integer deserialize(DataInputPlus in, int version) throws IOException
                 {
                     return (int) in.readUnsignedVInt();
                 }

                 @Override
                 public long serializedSize(Integer priority, int version)
                 {
                     return TypeSizes.sizeofUnsignedVInt(priority);
                 }
             });

    private final int id;
    private final InfoSerializer serializer;

    private static final Info[] ID_TO_INFO;

    static
    {
        Info[] infos = values();

        int max = -1;
        for (Info info : infos)
        {
            max = Math.max(max, info.id);
        }

        Info[] map = new Info[max + 1];

        for (Info info : infos)
        {
            if (map[info.id] != null)
            {
                throw new IllegalArgumentException("Cannot have two infos that map to the same id: " + info + " and " + map[info.id]);
            }

            map[info.id] = info;
        }

        ID_TO_INFO = map;
    }

    Info(int id, InfoSerializer serializer)
    {
        this.id = id;
        this.serializer = serializer;
    }

    public static Info fromId(int id)
    {
        Info info = id >= 0 && id < ID_TO_INFO.length ? ID_TO_INFO[id] : null;

        if (info == null)
        {
            throw new IllegalArgumentException("Unknown info id: " + id);
        }

        return info;
    }

    public int id()
    {
        return id;
    }

    public <T> InfoSerializer<T> serializer()
    {
        return (InfoSerializer<T>) serializer;
    }
}
