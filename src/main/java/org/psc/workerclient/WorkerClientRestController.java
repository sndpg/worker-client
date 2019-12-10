package org.psc.workerclient;

import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.transport.netty.client.TcpClientTransport;
import lombok.RequiredArgsConstructor;
import org.reactivestreams.Publisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.MediaType;
import org.springframework.messaging.rsocket.MetadataExtractor;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.messaging.rsocket.RSocketStrategies;
import org.springframework.util.MimeTypeUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.netty.tcp.TcpClient;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

@RequiredArgsConstructor
@RestController("/")
public class WorkerClientRestController {

    private final RSocketRequester rSocketRequester;

    @GetMapping
    public List<Double> getRandomNumber() {
        WebClient client = WebClient.builder().baseUrl("http://localhost:8080").build();

        Flux<Double> randomNumberResponse = client.get()
                .uri("/functional/flux/randomNumber")
                .retrieve()
                .bodyToFlux(Double.class)
                .filter(d -> d >= 0.5d)
                .limitRequest(50)
                .doOnEach(System.out::println);

        randomNumberResponse.publish().materialize();
        return randomNumberResponse.collectList().block();
    }

    @GetMapping("/mix")
    public Map<LocalDateTime, String> getMix() {
        WebClient client = WebClient.builder().baseUrl("http://localhost:8080").build();

        var resultMap = new TreeMap<LocalDateTime, String>();
        Flux<AbstractMap.SimpleEntry<LocalDateTime, String>> randomNumberResponse = client.get()
                .uri("/functional/flux/randomNumber")
                .retrieve()
                .bodyToFlux(Double.class)
                .filter(d -> d >= 0.5d)
                .map(e -> new AbstractMap.SimpleEntry<>(LocalDateTime.now(), String.valueOf(e)))
                .limitRequest(12)
                .doOnEach(System.out::println)
//                .doOnEach(e -> resultMap.put(e.get().getKey(), e.get().getValue()))
                .onErrorStop();

        Flux<AbstractMap.SimpleEntry<LocalDateTime, String>> timeResponse = client.get()
                .uri("functional/flux/time")
                .retrieve()
                .bodyToFlux(LocalDateTime.class)
                .map(e -> new AbstractMap.SimpleEntry<>(LocalDateTime.now(),
                        e.format(DateTimeFormatter.ofPattern("yyyyMMdd-hh:mm:ss.nnnnnnnnn"))))
                .limitRequest(7)
                .doOnEach(System.out::println)
//                .doOnEach(e -> resultMap.put(e.get().getKey(), e.get().getValue()))
                .onErrorStop();

        randomNumberResponse.subscribe(e -> resultMap.put(e.getKey(), e.getValue()));
        timeResponse.subscribe(e -> resultMap.put(e.getKey(), e.getValue()));

        Flux.merge(randomNumberResponse, timeResponse).blockLast();

        return resultMap;
    }

    @GetMapping(value = "/randomDecimals", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Publisher<BigDecimal> getRandomBigDecimals() {
        return rSocketRequester.route("randomDecimals")
                .retrieveFlux(BigDecimal.class);
    }

    @Configuration
    public static class RSocketConfiguration {

        @Bean
        public RSocket rSocket() {
            return RSocketFactory.connect()
                    .mimeType("message/x.rsocket.routing.v0", MediaType.APPLICATION_JSON_VALUE)
                    .frameDecoder(PayloadDecoder.ZERO_COPY)
                    .transport(TcpClientTransport.create(TcpClient.create().port(7777).host("localhost")))
                    .start()
                    .block();
        }

        @Bean
        public RSocketRequester rSocketRequester(RSocketStrategies rSocketStrategies) {
            return RSocketRequester.wrap(rSocket(), MimeTypeUtils.APPLICATION_JSON,
                    MimeTypeUtils.parseMimeType("message/x.rsocket.routing.v0"), rSocketStrategies);
        }
    }

}