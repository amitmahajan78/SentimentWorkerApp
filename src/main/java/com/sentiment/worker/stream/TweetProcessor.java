package com.sentiment.worker.stream;

import com.amazonaws.services.comprehend.AmazonComprehend;
import com.amazonaws.services.comprehend.model.DetectSentimentRequest;
import com.amazonaws.services.comprehend.model.DetectSentimentResult;
import com.sentiment.worker.dto.Tweet;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;
import org.springframework.util.MimeTypeUtils;

import java.text.DecimalFormat;
import java.text.ParseException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;


@Slf4j
@Component
public class TweetProcessor {

    AmazonComprehend amazonComprehend;
    TweetStreams tweetStreams;

    public TweetProcessor(AmazonComprehend amazonComprehend, TweetStreams tweetStreams) {
        this.amazonComprehend = amazonComprehend;
        this.tweetStreams = tweetStreams;
    }

    @StreamListener(TweetStreams.INPUT)
    public void process(@Payload Tweet t) throws ParseException {
        log.info("Received results from kafka: {}", t);
        HashMap<String, Float> sentiments = detectSentiments(base64Decoding(t.getTweet64()));

        sentiments.forEach((k, v) -> {
            log.info("Sending sentiment result to kafka for key " + k + " and value " + v);
            MessageChannel messageChannel = tweetStreams.outboundSentimentEvents();
            try {
                messageChannel.send(MessageBuilder
                        .withPayload(v)
                        .setHeader(KafkaHeaders.MESSAGE_KEY, parseTwitterDateTime(t.getTimestamp()).toString() + "-" + k)
                        .setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_JSON)
                        .build());
            } catch (ParseException e) {
                e.printStackTrace();
            }
        });

    }

    private Long parseTwitterDateTime(String twitterDateTime) throws ParseException {
        final String TWITTER = "EEE MMM dd HH:mm:ss ZZZ yyyy";
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(TWITTER);
        LocalDateTime dateTime = LocalDateTime.parse(twitterDateTime, formatter);
        ZonedDateTime zdt = ZonedDateTime.of(dateTime, ZoneId.systemDefault());
        return zdt.toInstant().truncatedTo(ChronoUnit.HOURS).toEpochMilli();
    }

    private HashMap<String, Float> detectSentiments(String tweet) {
        log.info("Calling DetectSentiment for ::" + tweet);
        DetectSentimentRequest detectSentimentRequest = new DetectSentimentRequest().withText(tweet)
                .withLanguageCode("en");
        DetectSentimentResult detectSentimentResult = amazonComprehend.detectSentiment(detectSentimentRequest);
        log.info(String.valueOf(detectSentimentResult));
        log.info("End of DetectSentiment\n");
        log.info("Done");

        return mapSentiment(detectSentimentResult);
    }

    private String base64Decoding(String encodedString) {
        byte[] decodedBytes = Base64.getDecoder().decode(encodedString);
        return new String(decodedBytes);
    }

    private HashMap<String, Float> mapSentiment(DetectSentimentResult detectSentimentResult) {

        DecimalFormat df = new DecimalFormat("#.###");

        Map<String, Float> sentimentMap = new HashMap<>();

        sentimentMap.put("Mixed", Float.valueOf(df.format(detectSentimentResult.getSentimentScore().getMixed() * 100)));
        sentimentMap.put("Negative", Float.valueOf(df.format(detectSentimentResult.getSentimentScore().getNegative() * 100)));
        sentimentMap.put("Neutral", Float.valueOf(df.format(detectSentimentResult.getSentimentScore().getNeutral() * 100)));
        sentimentMap.put("Positive", Float.valueOf(df.format(detectSentimentResult.getSentimentScore().getPositive() * 100)));

        return (HashMap<String, Float>) sentimentMap;
    }

}
