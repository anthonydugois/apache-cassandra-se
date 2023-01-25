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
import org.apache.cassandra.concurrent.ScheduledExecutors;
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

    private static final String UPDATE_INTERVAL_PROPERTY = "update_interval";
    private static final String DEFAULT_UPDATE_INTERVAL_PROPERTY = "100";

    private final int updateInterval;

    private static final String CONCURRENCY_WEIGHT_PROPERTY = "concurrency_weight";
    private static final String DEFAULT_CONCURRENCY_WEIGHT_PROPERTY = "1.0";

    private final double concurrencyWeight;

    private static final double ALPHA = 0.75;
    private static final int WINDOW_SIZE = 100;

    private volatile Map<InetAddressAndPort, Double> scores = new HashMap<>();
    private ConcurrentMap<InetAddressAndPort, ExponentiallyDecayingReservoir> latencies = new ConcurrentHashMap<>();
    private AtomicLongMap<InetAddressAndPort> outstanding = AtomicLongMap.create();

    public C3ScoringSelector(IEndpointSnitch snitch, Map<String, String> parameters)
    {
        super(snitch, parameters);

        this.updateInterval = Integer.parseInt(parameters.getOrDefault(UPDATE_INTERVAL_PROPERTY, DEFAULT_UPDATE_INTERVAL_PROPERTY));
        this.concurrencyWeight = Double.parseDouble(parameters.getOrDefault(CONCURRENCY_WEIGHT_PROPERTY, DEFAULT_CONCURRENCY_WEIGHT_PROPERTY));

        ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(this::updateScores, updateInterval, updateInterval, TimeUnit.MILLISECONDS);

        MessagingService.instance().latencySubscribers.subscribe(this);

        logger.info("Using {} with parameters {}", getClass().getName(), parameters);
    }

    private void updateScores()
    {
        ClusterState.instance.updateLocal();

        int size = latencies.size();

        Map<InetAddressAndPort, Snapshot> latencySnapshots = new HashMap<>(size);
        Map<InetAddressAndPort, Snapshot> queueSizeSnapshots = new HashMap<>(size);
        Map<InetAddressAndPort, Snapshot> serviceTimeSnapshots = new HashMap<>(size);

        for (InetAddressAndPort endpoint : latencies.keySet())
        {
            EndpointState state = ClusterState.instance.state(endpoint);

            if (state != null)
            {
                Snapshot latencySnapshot = latencies.get(endpoint).getSnapshot();
                Snapshot queueSizeSnapshot = ((ExponentiallyDecayingReservoir) state.get(Fact.PENDING_READS)).getSnapshot();
                Snapshot serviceTimeSnapshot = ((ServiceTimeAggregator.ServiceTime) state.get(Fact.SERVICE_TIME)).getSnapshot();

                latencySnapshots.put(endpoint, latencySnapshot);
                queueSizeSnapshots.put(endpoint, queueSizeSnapshot);
                serviceTimeSnapshots.put(endpoint, serviceTimeSnapshot);
            }
        }

        Map<InetAddressAndPort, Double> newScores = new HashMap<>();

        for (InetAddressAndPort endpoint : latencySnapshots.keySet())
        {
            long pending = Math.max(0, outstanding.get(endpoint));

            double averageLatency = latencySnapshots.get(endpoint).getMedian();
            double averageQueueSize = queueSizeSnapshots.get(endpoint).getMedian();
            double averageServiceTime = serviceTimeSnapshots.get(endpoint).getMedian();

            double estimatedQueueSize = 1 + pending * concurrencyWeight + averageQueueSize;
            double cubicQueueSize = estimatedQueueSize * estimatedQueueSize * estimatedQueueSize;

            double score = averageLatency - averageServiceTime + averageServiceTime * cubicQueueSize;

            newScores.put(endpoint, score);
        }

        scores = newScores;
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

    @Override
    public <C extends ReplicaCollection<? extends C>> C sortedByProximity(InetAddressAndPort address, C unsortedAddress, ReadOperation<SinglePartitionReadCommand> operation)
    {
        Map<InetAddressAndPort, Double> scores = this.scores;

        C sortedAddress = unsortedAddress.sorted((r1, r2) -> compareEndpoints(address, r1, r2, scores));

        outstanding.incrementAndGet(sortedAddress.get(0).endpoint());

        // logger.debug("Directing {} to {}", operation.key(), sortedAddress.get(0));

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
