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
import org.apache.cassandra.utils.FBUtilities;

public class C3ScoringSelector extends AbstractSelector implements LatencySubscribers.Subscriber
{
    private static final Logger logger = LoggerFactory.getLogger(C3ScoringSelector.class);

    private static final int WINDOW_SIZE = 100;
    private static final double ALPHA = 0.75;

    private ConcurrentMap<InetAddressAndPort, ExponentiallyDecayingReservoir> latencies = new ConcurrentHashMap<>();
    private AtomicLongMap<InetAddressAndPort> outstanding = AtomicLongMap.create();

    public C3ScoringSelector(IEndpointSnitch snitch, Map<String, String> parameters)
    {
        super(snitch, parameters);

        MessagingService.instance().latencySubscribers.subscribe(this);
    }

    @Override
    public void receiveTiming(InetAddressAndPort address, long latency, TimeUnit unit)
    {
        outstanding.decrementAndGet(address);

        ExponentiallyDecayingReservoir reservoir = latencies.get(address);

        if (reservoir == null)
        {
            ExponentiallyDecayingReservoir newReservoir = new ExponentiallyDecayingReservoir(WINDOW_SIZE, ALPHA);

            reservoir = latencies.putIfAbsent(address, newReservoir);

            if (reservoir == null)
            {
                reservoir = newReservoir;
            }
        }

        reservoir.update(unit.toMillis(latency));
    }

    public Map<InetAddressAndPort, Double> getScores(Iterable<Replica> replicas)
    {
        // ClusterState.instance.updateLocal();

        Map<InetAddressAndPort, Double> scores = new HashMap<>();

        for (Replica replica : replicas)
        {
            InetAddressAndPort address = replica.endpoint();

            if (address.equals(FBUtilities.getLocalAddressAndPort())) continue;

            EndpointState state = ClusterState.instance.state(address);

            double averageQueueSize = 0.0;
            double averageServiceTime = 0.0;

            if (state != null)
            {
                ExponentiallyDecayingReservoir queueSize = (ExponentiallyDecayingReservoir) state.get(Fact.PENDING_READS);
                ServiceTimeAggregator.ServiceTime serviceTime = (ServiceTimeAggregator.ServiceTime) state.get(Fact.SERVICE_TIME);

                averageQueueSize = queueSize.getSnapshot().getMedian();
                averageServiceTime = serviceTime.getSnapshot().getMedian();
            }

            ExponentiallyDecayingReservoir latency = latencies.get(address);

            double averageLatency = 0.0;

            if (latency != null)
            {
                averageLatency = latency.getSnapshot().getMedian();
            }

            double estimatedQueueSize = 1 + outstanding.get(address) * 2 + averageQueueSize;
            double cubicQueueSize = estimatedQueueSize * estimatedQueueSize * estimatedQueueSize;
            double score = averageLatency - averageServiceTime + averageServiceTime * cubicQueueSize;

            scores.put(address, score);
        }

        return scores;
    }

    @Override
    public <C extends ReplicaCollection<? extends C>> C sortedByProximity(InetAddressAndPort address, C unsortedAddress, ReadOperation<SinglePartitionReadCommand> operation)
    {
        Map<InetAddressAndPort, Double> scores = getScores(unsortedAddress);

        C sortedAddress = super.sortedByProximity(address, unsortedAddress);

        InetAddressAndPort chosen = sortedAddress.get(0).endpoint();
        outstanding.incrementAndGet(chosen);

        return sortedAddress;
    }

    @Override
    public int compareEndpoints(InetAddressAndPort target, Replica r1, Replica r2)
    {
        return snitch.compareEndpoints(target, r1, r2);
    }
}
