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
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.util.concurrent.AtomicLongMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fr.ens.cassandra.se.op.ReadOperation;
import fr.ens.cassandra.se.op.info.Info;
import io.netty.util.internal.ThreadLocalRandom;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaCollection;

public class PopularityAwarePrimarySelector extends AbstractSelector
{
    private static final Logger logger = LoggerFactory.getLogger(PopularityAwarePrimarySelector.class);

    private static final String THRESHOLD_PROPERTY = "threshold";
    private static final String DEFAULT_THRESHOLD_PROPERTY = "0.0";

    private final double threshold;

    private final AtomicLongMap<Long> accesses = AtomicLongMap.create();
    private final AtomicInteger total = new AtomicInteger();

    public PopularityAwarePrimarySelector(IEndpointSnitch snitch, Map<String, String> parameters)
    {
        super(snitch, parameters);

        this.threshold = Double.parseDouble(parameters.getOrDefault(THRESHOLD_PROPERTY, DEFAULT_THRESHOLD_PROPERTY));

        logger.info("Using {} with parameters {}", getClass().getName(), parameters);
    }

    @Override
    public <C extends ReplicaCollection<? extends C>> C sortedByProximity(InetAddressAndPort address, C unsortedAddress, ReadOperation<SinglePartitionReadCommand> operation)
    {
        // Compute a positive hash value for the current key.
        // Note that we need to explicitly cast the hash code to a long value, otherwise the integer hash will first
        // overflow, then loop back to negative integers and finally be casted to a negative long, which is not wanted.
        long hash = (long) operation.key().hashCode() + Integer.MAX_VALUE;

        if (hash < 0)
        {
            throw new UnsupportedOperationException();
        }

        // Here we do +1 because the current operation counts as new access.
        // This also permits to avoid dividing by 0.
        long value = accesses.incrementAndGet(hash);
        long count = total.incrementAndGet();

        double frequency = (double) value / count;

        C sortedAddress;

        if (frequency > threshold)
        {
            // This key is considered popular.
            // Let us randomize (uniform) the replica selection to load balance accesses.
            int index = ThreadLocalRandom.current().nextInt(unsortedAddress.size());
            Replica replica = unsortedAddress.get(index);

            sortedAddress = unsortedAddress.sorted((r1, r2) -> {
                if (r1.equals(replica))
                {
                    return -1;
                }

                if (r2.equals(replica))
                {
                    return 1;
                }

                return compareEndpoints(address, r1, r2);
            });

            // The corresponding operation should have higher priority.
            operation.add(Info.PRIORITY, 0);

            // logger.debug("Key {} is popular", operation.key());
        }
        else
        {
            // This key is not considered popular.
            // Use the default policy (Primary by default).
            sortedAddress = super.sortedByProximity(address, unsortedAddress);

            // The corresponding operation has lower priority.
            operation.add(Info.PRIORITY, 1);

            // logger.debug("Key {} is not popular", operation.key());
        }

        // logger.debug("Directing {} to {}", operation.key(), sortedAddress.get(0));

        return sortedAddress;
    }

    @Override
    public int compareEndpoints(InetAddressAndPort target, Replica r1, Replica r2)
    {
        return snitch.compareEndpoints(target, r1, r2);
    }
}
