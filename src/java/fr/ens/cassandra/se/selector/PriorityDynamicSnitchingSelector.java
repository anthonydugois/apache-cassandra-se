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
import fr.ens.cassandra.se.op.info.Info;
import fr.ens.cassandra.se.oracle.Oracle;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.ReplicaCollection;

public class PriorityDynamicSnitchingSelector extends DynamicSnitchingSelector
{
    private static final Logger logger = LoggerFactory.getLogger(PriorityDynamicSnitchingSelector.class);

    private static final String THRESHOLD_PROPERTY = "threshold";
    private static final String DEFAULT_THRESHOLD_PROPERTY = "1024";

    private final int threshold;

    public PriorityDynamicSnitchingSelector(IEndpointSnitch snitch, Map<String, String> parameters)
    {
        super(snitch, parameters);

        this.threshold = Integer.parseInt(parameters.getOrDefault(THRESHOLD_PROPERTY, DEFAULT_THRESHOLD_PROPERTY));

        logger.info("Using {} with parameters {}", getClass().getName(), parameters);
    }

    @Override
    public <C extends ReplicaCollection<? extends C>> C sortedByProximity(InetAddressAndPort address, C unsortedAddress, ReadOperation<SinglePartitionReadCommand> operation)
    {
        Oracle<String, Integer> oracle = DatabaseDescriptor.getOracle("size");

        int size = oracle.get(operation.key());

        if (size > 0)
        {
            if (size <= threshold)
            {
                operation.add(Info.PRIORITY, 0);

                // logger.debug("Adding priority {} to key {} of size {}", 0, operation.key(), size);
            }
            else
            {
                operation.add(Info.PRIORITY, 1);

                // logger.debug("Adding priority {} to key {} of size {}", 1, operation.key(), size);
            }
        }
        else
        {
            operation.add(Info.PRIORITY, 0);

            // logger.debug("Adding default priority to key {}", operation.key());
        }

        C sortedAddress = super.sortedByProximity(address, unsortedAddress);

        // logger.debug("Directing {} to {}", operation.key(), sortedAddress.get(0));

        return sortedAddress;
    }
}
