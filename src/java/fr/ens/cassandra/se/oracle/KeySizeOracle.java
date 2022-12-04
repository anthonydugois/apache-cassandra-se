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

package fr.ens.cassandra.se.oracle;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import fr.ens.cassandra.se.oracle.loader.CachedPathLoader;
import org.apache.cassandra.concurrent.ScheduledExecutors;

public class KeySizeOracle extends AbstractOracle<String, Integer>
{
    public static final String DIR_PROPERTY = "dir";
    public static final String DEFAULT_DIR_PROPERTY = "/";

    public static final String GLOB_PROPERTY = "glob";
    public static final String DEFAULT_GLOB_PROPERTY = "*";

    public static final String DELAY_PROPERTY = "delay";
    public static final String DEFAULT_DELAY_PROPERTY = "10";

    private final long delay;

    private final CachedPathLoader loader;

    private final Map<String, Integer> sizes = new HashMap<>();

    public KeySizeOracle(Map<String, String> parameters)
    {
        super(parameters);

        String dir = parameters.getOrDefault(DIR_PROPERTY, DEFAULT_DIR_PROPERTY);
        String glob = parameters.getOrDefault(GLOB_PROPERTY, DEFAULT_GLOB_PROPERTY);

        this.delay = Long.parseLong(parameters.getOrDefault(DELAY_PROPERTY, DEFAULT_DELAY_PROPERTY));

        this.loader = new CachedPathLoader(dir, glob);
    }

    private void load()
    {
        for (Path path : loader.getPaths())
        {
            try (Stream<String> lines = Files.lines(path))
            {
                lines.forEach((line) -> {
                    String[] cells = line.split(",", 2);
                    sizes.put(cells[0], Integer.parseInt(cells[1]));
                });
            }
            catch (IOException exception)
            {
                // ignored
            }
        }
    }

    @Override
    public void init()
    {
        ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(this::load, 0, delay, TimeUnit.SECONDS);
    }

    @Override
    public Integer get(String key)
    {
        return sizes.get(key);
    }
}
