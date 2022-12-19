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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.AtomicLongMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Snapshot;
import fr.ens.cassandra.se.op.ReadOperation;
import fr.ens.cassandra.se.state.ClusterState;
import fr.ens.cassandra.se.state.EndpointState;
import fr.ens.cassandra.se.state.facts.Fact;
import fr.ens.cassandra.se.state.facts.impl.ServiceTimeAggregator;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaCollection;
import org.apache.cassandra.net.LatencySubscribers;
import org.apache.cassandra.net.MessagingService;

public class C3ScoringSelector extends AbstractSelector implements LatencySubscribers.Subscriber
{
    private static final Logger logger = LoggerFactory.getLogger(C3ScoringSelector.class);

    private static final String CONCURRENCY_WEIGHT_PROPERTY = "concurrency_weight";
    private static final String DEFAULT_CONCURRENCY_WEIGHT_PROPERTY = "1.0";

    private final double concurrencyWeight;

    private ConcurrentMap<InetAddressAndPort, ExponentiallyDecayingReservoir> latencies = new ConcurrentHashMap<>();
    private AtomicLongMap<InetAddressAndPort> outstanding = AtomicLongMap.create();

    public C3ScoringSelector(IEndpointSnitch snitch, Map<String, String> parameters)
    {
        super(snitch, parameters);

        this.concurrencyWeight = Double.parseDouble(parameters.getOrDefault(CONCURRENCY_WEIGHT_PROPERTY, DEFAULT_CONCURRENCY_WEIGHT_PROPERTY));

        MessagingService.instance().latencySubscribers.subscribe(this);
    }

    @Override
    public void receiveTiming(InetAddressAndPort address, long latency, TimeUnit unit)
    {
        outstanding.decrementAndGet(address);

        ExponentiallyDecayingReservoir reservoir = latencies.get(address);

        if (reservoir == null)
        {
            ExponentiallyDecayingReservoir newReservoir = new ExponentiallyDecayingReservoir();

            reservoir = latencies.putIfAbsent(address, newReservoir);

            if (reservoir == null)
            {
                reservoir = newReservoir;
            }
        }

        reservoir.update(unit.toMillis(latency));
    }

    public Map<InetAddressAndPort, Double> getScores(InetAddressAndPort address, Iterable<Replica> replicas)
    {
        ClusterState.instance.updateLocal(address);

        Map<InetAddressAndPort, Double> scores = new HashMap<>();

        for (Replica replica : replicas)
        {
            InetAddressAndPort endpoint = replica.endpoint();
            EndpointState state = ClusterState.instance.state(endpoint);

            double score;

            double averageQueueSize = 0.0;
            double averageServiceTime = 0.0;

            if (state != null)
            {
                Snapshot queueSizes = ((ExponentiallyDecayingReservoir) state.get(Fact.PENDING_READS)).getSnapshot();
                Snapshot serviceTimes = ((ServiceTimeAggregator.ServiceTime) state.get(Fact.SERVICE_TIME)).getSnapshot();

                if (queueSizes.size() > 0)
                {
                    averageQueueSize = queueSizes.getMean();
                }

                if (serviceTimes.size() > 0)
                {
                    averageServiceTime = serviceTimes.getMean();
                }
            }

            if (endpoint.equals(address))
            {
                // This is the local endpoint
                double estimatedQueueSize = 1 + averageQueueSize;
                double cubicQueueSize = estimatedQueueSize * estimatedQueueSize * estimatedQueueSize;

                score = averageServiceTime * cubicQueueSize;
            }
            else
            {
                // This is a remote endpoint
                ExponentiallyDecayingReservoir latency = latencies.get(endpoint);

                double averageLatency = 0.0;

                if (latency != null)
                {
                    averageLatency = latency.getSnapshot().getMean();
                }

                double estimatedQueueSize = 1 + outstanding.get(endpoint) * concurrencyWeight + averageQueueSize;
                double cubicQueueSize = estimatedQueueSize * estimatedQueueSize * estimatedQueueSize;

                score = averageLatency - averageServiceTime + averageServiceTime * cubicQueueSize;
            }

            scores.put(endpoint, score);
        }

        return scores;
    }

    @Override
    public <C extends ReplicaCollection<? extends C>> C sortedByProximity(InetAddressAndPort address, C unsortedAddress, ReadOperation<SinglePartitionReadCommand> operation)
    {
        Map<InetAddressAndPort, Double> scores = getScores(address, unsortedAddress);

        C sortedAddress = unsortedAddress.sorted((r1, r2) -> compareEndpoints(address, r1, r2, scores));

        InetAddressAndPort first = sortedAddress.get(0).endpoint();

        if (!first.equals(address))
        {
            outstanding.incrementAndGet(first);
        }

        return sortedAddress;
    }

    public int compareEndpoints(InetAddressAndPort target, Replica r1, Replica r2, Map<InetAddressAndPort, Double> scores)
    {
        Double score1 = scores.getOrDefault(r1.endpoint(), 0.0);
        Double score2 = scores.getOrDefault(r2.endpoint(), 0.0);

        if (score1.equals(score2))
        {
            return snitch.compareEndpoints(target, r1, r2);
        }

        return score1.compareTo(score2);
    }

    @Override
    public int compareEndpoints(InetAddressAndPort target, Replica r1, Replica r2)
    {
        return snitch.compareEndpoints(target, r1, r2);
    }
}
