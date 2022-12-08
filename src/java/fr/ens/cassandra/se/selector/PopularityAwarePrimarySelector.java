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

package fr.ens.cassandra.se.selector;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fr.ens.cassandra.se.op.ReadOperation;
import org.HdrHistogram.Recorder;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaCollection;

public class PopularityAwarePrimarySelector extends AbstractSelector
{
    private static final Logger logger = LoggerFactory.getLogger(PopularityAwarePrimarySelector.class);

    private final Recorder recorder = new Recorder(2);

    public PopularityAwarePrimarySelector(IEndpointSnitch snitch, Map<String, String> parameters)
    {
        super(snitch, parameters);
    }

    @Override
    public <C extends ReplicaCollection<? extends C>> C sortedByProximity(InetAddressAndPort address, C unsortedAddress, ReadOperation<SinglePartitionReadCommand> operation)
    {
        long token = (long) operation.command().partitionKey().getToken().getTokenValue();

        recorder.recordValue(token);

        return super.sortedByProximity(address, unsortedAddress, operation);
    }

    @Override
    public int compareEndpoints(InetAddressAndPort target, Replica r1, Replica r2)
    {
        return snitch.compareEndpoints(target, r1, r2);
    }
}
