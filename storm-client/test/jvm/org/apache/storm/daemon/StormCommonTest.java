/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.daemon;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.Set;
import org.apache.storm.Constants;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.Grouping;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.GeneralTopologyContext;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.testing.TestWordCounter;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.junit.jupiter.api.Test;

public class StormCommonTest {
    @Test
    public void testUpstreamFeedbackSourceComponentsFiltersSystemSourcesAndDeduplicatesComponents() {
        GeneralTopologyContext context = mock(GeneralTopologyContext.class);
        when(context.getSources("sink")).thenReturn(Map.of(
                new GlobalStreamId("source", "first"), new Grouping(),
                new GlobalStreamId("source", "second"), new Grouping(),
                new GlobalStreamId("other-source", StormCommon.SYSTEM_STREAM_ID), new Grouping(),
                new GlobalStreamId(Acker.ACKER_COMPONENT_ID, "ack"), new Grouping(),
                new GlobalStreamId(Constants.SYSTEM_COMPONENT_ID, StormCommon.SYSTEM_STREAM_ID), new Grouping()));

        assertEquals(Set.of("source", "other-source"), StormCommon.upstreamFeedbackSourceComponents(context, "sink"));
    }

    @Test
    public void testUpstreamFeedbackSourceComponentsForSystemComponent() {
        GeneralTopologyContext context = mock(GeneralTopologyContext.class);
        when(context.getSources(Acker.ACKER_COMPONENT_ID)).thenReturn(Map.of(
                new GlobalStreamId("source", "default"), new Grouping()));

        assertEquals(Set.of(), StormCommon.upstreamFeedbackSourceComponents(context, Acker.ACKER_COMPONENT_ID));
    }

    @Test
    public void testUpstreamFeedbackSourceComponentsWithoutSources() {
        GeneralTopologyContext context = mock(GeneralTopologyContext.class);
        when(context.getSources("spout")).thenReturn(Map.of());

        assertEquals(Set.of(), StormCommon.upstreamFeedbackSourceComponents(context, "spout"));
    }

    @Test
    public void testUpstreamFeedbackSourceComponentsWithOnlySystemSources() {
        GeneralTopologyContext context = mock(GeneralTopologyContext.class);
        when(context.getSources("sink")).thenReturn(Map.of(
                new GlobalStreamId(Acker.ACKER_COMPONENT_ID, "ack"), new Grouping()));

        assertEquals(Set.of(), StormCommon.upstreamFeedbackSourceComponents(context, "sink"));
    }

    @Test
    public void testValidateStructureChecksFieldsGroupingAgainstSubscribedStream() {
        // "id" is a field of the "control" stream, not of the default stream
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new TwoStreamSpout());
        builder.setBolt("bolt", new TestWordCounter()).fieldsGrouping("spout", new Fields("id"));

        InvalidTopologyException e = assertThrows(InvalidTopologyException.class,
                () -> StormCommon.validateStructure(builder.createTopology()));
        assertEquals("Component: [bolt] subscribes from stream: [default] of component [spout] "
                + "with non-existent fields: [id]", e.get_msg());
    }

    @Test
    public void testValidateStructureAcceptsFieldsOfSubscribedStream() throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new TwoStreamSpout());
        builder.setBolt("words", new TestWordCounter()).fieldsGrouping("spout", new Fields("word"));
        builder.setBolt("ids", new TestWordCounter())
                .fieldsGrouping("spout", "control", new Fields("id"));

        StormCommon.validateStructure(builder.createTopology());
    }

    private static class TwoStreamSpout extends BaseRichSpout {
        @Override
        public void open(Map<String, Object> conf, TopologyContext context,
                SpoutOutputCollector collector) {
        }

        @Override
        public void nextTuple() {
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
            declarer.declareStream("control", new Fields("id"));
        }
    }
}
