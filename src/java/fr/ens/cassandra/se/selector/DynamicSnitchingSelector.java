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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Snapshot;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaCollection;
import org.apache.cassandra.net.LatencySubscribers;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;

public class DynamicSnitchingSelector extends AbstractSelector implements LatencySubscribers.Subscriber
{
    private static final Logger logger = LoggerFactory.getLogger(DynamicSnitchingSelector.class);

    public static final String UPDATE_INTERVAL_PROPERTY = "update_interval";
    public static final String DEFAULT_UPDATE_INTERVAL_PROPERTY = "100";

    private final int updateInterval;

    public static final String RESET_INTERVAL_PROPERTY = "reset_interval";
    public static final String DEFAULT_RESET_INTERVAL_PROPERTY = "600000";

    private final int resetInterval;

    public static final String BADNESS_THRESHOLD_PROPERTY = "badness_threshold";
    public static final String DEFAULT_BADNESS_THRESHOLD_PROPERTY = "1.0";

    private final double badnessThreshold;

    private static final boolean USE_SEVERITY = !Boolean.getBoolean("cassandra.ignore_dynamic_snitch_severity");

    private static final double ALPHA = 0.75;
    private static final int WINDOW_SIZE = 100;
    private static final double RANGE_MERGING_PREFERENCE = 1.5;

    private boolean registered = false;

    private volatile Map<InetAddressAndPort, Double> scores = new HashMap<>();
    private final ConcurrentMap<InetAddressAndPort, ExponentiallyDecayingReservoir> samples = new ConcurrentHashMap<>();

    public DynamicSnitchingSelector(IEndpointSnitch snitch, Map<String, String> parameters)
    {
        super(snitch, parameters);

        this.updateInterval = Integer.parseInt(parameters.getOrDefault(UPDATE_INTERVAL_PROPERTY, DEFAULT_UPDATE_INTERVAL_PROPERTY));
        this.resetInterval = Integer.parseInt(parameters.getOrDefault(RESET_INTERVAL_PROPERTY, DEFAULT_RESET_INTERVAL_PROPERTY));
        this.badnessThreshold = Double.parseDouble(parameters.getOrDefault(BADNESS_THRESHOLD_PROPERTY, DEFAULT_BADNESS_THRESHOLD_PROPERTY));

        ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(this::updateScores, updateInterval, updateInterval, TimeUnit.MILLISECONDS);
        ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(this::resetSamples, resetInterval, resetInterval, TimeUnit.MILLISECONDS);
    }

    public int getUpdateInterval()
    {
        return updateInterval;
    }

    public int getResetInterval()
    {
        return resetInterval;
    }

    public double getBadnessThreshold()
    {
        return badnessThreshold;
    }

    private double getSeverity(InetAddressAndPort endpoint)
    {
        EndpointState state = Gossiper.instance.getEndpointStateForEndpoint(endpoint);

        if (state == null)
        {
            return 0.0;
        }

        VersionedValue event = state.getApplicationState(ApplicationState.SEVERITY);

        if (event == null)
        {
            return 0.0;
        }

        return Double.parseDouble(event.value);
    }

    private void updateScores()
    {
        if (!StorageService.instance.isGossipActive())
        {
            return;
        }

        if (!registered && MessagingService.instance() != null)
        {
            MessagingService.instance().latencySubscribers.subscribe(this);

            registered = true;
        }

        Map<InetAddressAndPort, Snapshot> snapshots = new HashMap<>(samples.size());

        for (Map.Entry<InetAddressAndPort, ExponentiallyDecayingReservoir> entry : samples.entrySet())
        {
            snapshots.put(entry.getKey(), entry.getValue().getSnapshot());
        }

        double maxLatency = 1;

        for (Map.Entry<InetAddressAndPort, Snapshot> entry : snapshots.entrySet())
        {
            double mean = entry.getValue().getMedian();

            if (mean > maxLatency)
            {
                maxLatency = mean;
            }
        }

        Map<InetAddressAndPort, Double> newScores = new HashMap<>();

        for (Map.Entry<InetAddressAndPort, Snapshot> entry : snapshots.entrySet())
        {
            double score = entry.getValue().getMedian() / maxLatency;

            if (USE_SEVERITY)
            {
                score += getSeverity(entry.getKey());
            }

            newScores.put(entry.getKey(), score);
        }

        scores = newScores;
    }

    private void resetSamples()
    {
        samples.clear();
    }

    private double maxScore(ReplicaCollection<?> endpoints)
    {
        double maxScore = -1.0;

        for (Replica replica : endpoints)
        {
            Double score = scores.get(replica.endpoint());

            if (score == null)
            {
                continue;
            }

            if (score > maxScore)
            {
                maxScore = score;
            }
        }

        return maxScore;
    }

    @Override
    public void receiveTiming(InetAddressAndPort host, long latency, TimeUnit unit)
    {
        ExponentiallyDecayingReservoir sample = samples.get(host);

        if (sample == null)
        {
            ExponentiallyDecayingReservoir newSample = new ExponentiallyDecayingReservoir(WINDOW_SIZE, ALPHA);

            sample = samples.putIfAbsent(host, newSample);

            if (sample == null)
            {
                sample = newSample;
            }
        }

        sample.update(unit.toMillis(latency));
    }

    @Override
    public <C extends ReplicaCollection<? extends C>> C sortedByProximity(InetAddressAndPort address, C unsortedAddress)
    {
        return badnessThreshold == 0 ? sortedByProximityWithScore(address, unsortedAddress) : sortedByProximityWithBadness(address, unsortedAddress);
    }

    private <C extends ReplicaCollection<? extends C>> C sortedByProximityWithScore(InetAddressAndPort address, C unsortedAddress)
    {
        Map<InetAddressAndPort, Double> scores = this.scores;

        return unsortedAddress.sorted((r1, r2) -> compareEndpoints(address, r1, r2, scores));
    }

    private <C extends ReplicaCollection<? extends C>> C sortedByProximityWithBadness(InetAddressAndPort address, C unsortedAddress)
    {
        if (unsortedAddress.size() < 2)
        {
            return unsortedAddress;
        }

        unsortedAddress = snitch.sortedByProximity(address, unsortedAddress);

        Map<InetAddressAndPort, Double> scores = this.scores;

        List<Double> subsnitchOrderedScores = new ArrayList<>(unsortedAddress.size());

        for (Replica replica : unsortedAddress)
        {
            Double score = scores.getOrDefault(replica.endpoint(), 0.0);

            subsnitchOrderedScores.add(score);
        }

        List<Double> sortedScores = new ArrayList<>(subsnitchOrderedScores);

        Collections.sort(sortedScores);

        Iterator<Double> sortedScoreIterator = sortedScores.iterator();

        for (Double subsnitchScore : subsnitchOrderedScores)
        {
            if (subsnitchScore > (sortedScoreIterator.next() * (1.0 + badnessThreshold)))
            {
                return sortedByProximityWithScore(address, unsortedAddress);
            }
        }

        return unsortedAddress;
    }

    @Override
    public int compareEndpoints(InetAddressAndPort target, Replica r1, Replica r2)
    {
        throw new UnsupportedOperationException("Unsafe operation: method is not stable for identical arguments as " +
                                                "scores can change at any time");
    }

    private int compareEndpoints(InetAddressAndPort target, Replica r1, Replica r2, Map<InetAddressAndPort, Double> scores)
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
    public boolean isWorthMergingForRangeQuery(ReplicaCollection<?> merged, ReplicaCollection<?> l1, ReplicaCollection<?> l2)
    {
        if (!snitch.isWorthMergingForRangeQuery(merged, l1, l2))
        {
            return false;
        }

        if (l1.size() == 1 && l2.size() == 1 && l1.get(0).equals(l2.get(0)))
        {
            return true;
        }

        double maxMerged = maxScore(merged);
        double maxL1 = maxScore(l1);
        double maxL2 = maxScore(l2);

        if (maxMerged < 0 || maxL1 < 0 || maxL2 < 0)
        {
            return true;
        }

        return maxMerged <= (maxL1 + maxL2) * RANGE_MERGING_PREFERENCE;
    }
}
