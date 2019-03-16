package org.psc.workerclient;

import org.springframework.http.HttpMethod;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RestController("/")
public class WorkerClientRestController {

    @GetMapping
    public List<Double> getRandomNumber() {
        WebClient client = WebClient.builder().baseUrl("http://localhost:8080").build();

        Flux<Double> response = client.get()
                .uri("/functional/flux/randomNumber")
                .retrieve()
                .bodyToFlux(Double.class)
                .filter(d -> d >= 0.5d)
                .limitRequest(50)
                .doOnEach(System.out::println);

        response.publish().materialize();
        return response.collectList().block();
    }
}