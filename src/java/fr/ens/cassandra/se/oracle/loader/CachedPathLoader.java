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

package fr.ens.cassandra.se.oracle.loader;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;

import com.google.common.collect.Sets;

public class CachedPathLoader
{
    private final String dir;
    private final String glob;

    private final Set<Path> cachedPaths = Sets.newHashSet();

    public CachedPathLoader(String dir, String glob)
    {
        this.dir = dir;
        this.glob = glob;
    }

    public String getDir()
    {
        return dir;
    }

    public String getGlob()
    {
        return glob;
    }

    public Set<Path> getCachedPaths()
    {
        return cachedPaths;
    }

    public Set<Path> getPaths()
    {
        Set<Path> paths = Sets.newHashSet();

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(dir), glob))
        {
            for (Path path : stream)
            {
                if (!cachedPaths.contains(path))
                {
                    paths.add(path);
                    cachedPaths.add(path);
                }
            }
        }
        catch (IOException exception)
        {
            // ignored
        }

        return paths;
    }
}
