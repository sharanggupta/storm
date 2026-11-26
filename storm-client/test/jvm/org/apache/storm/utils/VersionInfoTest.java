/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.storm.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class VersionInfoTest {
    private static final String PROPERTIES_NAME = "storm-client-version-info.properties";

    @TempDir
    Path tempDir;

    @Test
    public void testFirstJarOnClasspathWins() throws IOException {
        String first = jarWithVersion("first.jar", "2.6.0");
        String second = jarWithVersion("second.jar", "1.2.3");

        assertEquals("2.6.0", VersionInfo.getFromClasspath(Arrays.asList(first, second)).getVersion());
        assertEquals("1.2.3", VersionInfo.getFromClasspath(Arrays.asList(second, first)).getVersion());
    }

    @Test
    public void testFirstEntryWinsForJarsAndDirectories() throws IOException {
        String jar = jarWithVersion("client.jar", "2.6.0");
        String dir = dirWithVersion("classes", "1.2.3");

        assertEquals("2.6.0", VersionInfo.getFromClasspath(Arrays.asList(jar, dir)).getVersion());
        assertEquals("1.2.3", VersionInfo.getFromClasspath(Arrays.asList(dir, jar)).getVersion());
    }

    @Test
    public void testJarWithoutVersionInfoIsSkipped() throws IOException {
        Path empty = tempDir.resolve("empty.jar");
        try (JarOutputStream out = new JarOutputStream(Files.newOutputStream(empty))) {
            out.putNextEntry(new JarEntry("other.properties"));
            out.closeEntry();
        }
        String versioned = jarWithVersion("client.jar", "2.6.0");

        assertEquals("2.6.0",
            VersionInfo.getFromClasspath(Arrays.asList(empty.toString(), versioned)).getVersion());
        assertNull(VersionInfo.getFromClasspath(Collections.singletonList(empty.toString())));
    }

    private String jarWithVersion(String name, String version) throws IOException {
        Path jar = tempDir.resolve(name);
        try (JarOutputStream out = new JarOutputStream(Files.newOutputStream(jar))) {
            out.putNextEntry(new JarEntry(PROPERTIES_NAME));
            writeVersion(out, version);
            out.closeEntry();
        }
        return jar.toString();
    }

    private String dirWithVersion(String name, String version) throws IOException {
        Path dir = Files.createDirectory(tempDir.resolve(name));
        try (OutputStream out = Files.newOutputStream(dir.resolve(PROPERTIES_NAME))) {
            writeVersion(out, version);
        }
        return dir.toString();
    }

    private static void writeVersion(OutputStream out, String version) throws IOException {
        out.write(("version=" + version + "\n").getBytes(StandardCharsets.UTF_8));
    }
}
