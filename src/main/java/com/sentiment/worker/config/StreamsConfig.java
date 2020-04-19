package com.sentiment.worker.config;

import com.sentiment.worker.stream.TweetStreams;
import org.springframework.cloud.stream.annotation.EnableBinding;

@EnableBinding(TweetStreams.class)
public class StreamsConfig {
}
