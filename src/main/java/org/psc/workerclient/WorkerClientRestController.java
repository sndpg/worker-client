package org.psc.workerclient;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

@RestController("/")
public class WorkerClientRestController {

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
                .map(e -> new AbstractMap.SimpleEntry<>(LocalDateTime.now(), e.format(DateTimeFormatter.ofPattern("yyyyMMdd-hh:mm:ss.nnnnnnnnn"))))
                .limitRequest(7)
                .doOnEach(System.out::println)
//                .doOnEach(e -> resultMap.put(e.get().getKey(), e.get().getValue()))
                .onErrorStop();

        randomNumberResponse.subscribe(e -> resultMap.put(e.getKey(), e.getValue()));
        timeResponse.subscribe(e -> resultMap.put(e.getKey(), e.getValue()));

        Flux.merge(randomNumberResponse, timeResponse).blockLast();

        return resultMap;
    }

}