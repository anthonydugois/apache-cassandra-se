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

import java.nio.charset.StandardCharsets;

import fr.ens.cassandra.se.op.info.Info;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.SinglePartitionReadCommand;

public class ReadOperation<T extends ReadCommand>
{
    private final T command;

    private String key = null;

    public ReadOperation(T command)
    {
        this.command = command;

        if (command instanceof SinglePartitionReadCommand)
        {
            this.key = new String(((SinglePartitionReadCommand) command).partitionKey().getKey().array(), StandardCharsets.UTF_8);
        }
    }

    public static <T extends ReadCommand> ReadOperation<T> from(T command)
    {
        return new ReadOperation<>(command);
    }

    public T command()
    {
        return command;
    }

    public String key()
    {
        return key;
    }

    public boolean has(Info info)
    {
        return command.infos().has(info);
    }

    public Object info(Info info)
    {
        return command.infos().get(info);
    }

    public void add(Info info, Object value)
    {
        command.infos().put(info, value);
    }
}
