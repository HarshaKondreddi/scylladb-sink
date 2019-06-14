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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.channel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Splitter;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Round-robin channel selector. This selector allows the event to be placed in channels in a round-robin manner
 * that the source is configured with. Like @{@link ReplicatingChannelSelector} this selector also can be configured
 * with "optional" channels. When configured with both required optional channels, this channel selector will
 * select one channel from required channels and one channel from optional channels.
 */
public class RoundRobinChannelSelector extends AbstractChannelSelector {
  private static Logger LOGGER = LoggerFactory.getLogger(RoundRobinChannelSelector.class);

  private static final List<Channel> EMPTY_LIST = Collections.emptyList();
  public static final String CONFIG_OPTIONAL = "optional";

  private List<Channel> requiredChannels;
  private AtomicInteger requiredChannelIndex;

  private List<Channel> optionalChannels;
  private AtomicInteger optionalChannelIndex;

  @Override
  public List<Channel> getRequiredChannels(Event event) {
    if (requiredChannels.size() == 0) return EMPTY_LIST;
    int currentIndex = Math.abs(requiredChannelIndex.getAndIncrement() % requiredChannels.size());
    return requiredChannels.subList(currentIndex, currentIndex + 1);
  }

  @Override
  public List<Channel> getOptionalChannels(Event event) {
    if (optionalChannels.size() == 0) return EMPTY_LIST;
    int currentIndex = Math.abs(optionalChannelIndex.getAndIncrement() % optionalChannels.size());
    return optionalChannels.subList(currentIndex, currentIndex + 1);
  }

  @Override
  public void configure(Context context) {
    this.requiredChannels = new ArrayList<>(getAllChannels());
    this.requiredChannelIndex = new AtomicInteger(0);

    this.optionalChannels = new ArrayList<>();
    this.optionalChannelIndex = new AtomicInteger(0);

    Map<String, Channel> channelNameMap = getChannelNameMap();
    for (String optional : Splitter.on(" ").omitEmptyStrings().trimResults()
        .split(context.getString(CONFIG_OPTIONAL, ""))) {
      Channel optionalChannel = channelNameMap.get(optional);
      requiredChannels.remove(optionalChannel);
      if (!optionalChannels.contains(optionalChannel)) {
        optionalChannels.add(optionalChannel);
      } else {
        LOGGER.warn("found duplicate optional channel configured: {}", optional);
      }
    }

    LOGGER.info("initialized round robin channel selector. required channels configured: {}," +
        " optional channels configured: {}", requiredChannels.size(), optionalChannels.size());
  }
}
