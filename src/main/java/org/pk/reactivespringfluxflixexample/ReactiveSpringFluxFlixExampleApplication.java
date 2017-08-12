package org.pk.reactivespringfluxflixexample;

import static java.util.UUID.randomUUID;
import static org.springframework.web.reactive.function.server.RequestPredicates.GET;
import static org.springframework.web.reactive.function.server.RouterFunctions.route;

import java.time.Duration;
import java.util.Date;
import java.util.stream.Stream;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@SpringBootApplication
public class ReactiveSpringFluxFlixExampleApplication {

	public static void main(String[] args) {
		SpringApplication.run(ReactiveSpringFluxFlixExampleApplication.class,
				args);
	}

	@Bean
	public CommandLineRunner movies(MovieRepository repo) {
		return args -> repo
				.deleteAll()
				.subscribe(
						null,
						null,
						() -> Stream
								.of("Silence of the Lambdas, Dunkirk, Cast Away, Forest Gump, Borat, Life of pi"
										.split(","))
								.map(moviewTitle -> new Movie(randomUUID()
										.toString(), moviewTitle))
								.forEach(
										movie -> repo.save(movie).subscribe(
												System.out::println)));

	}
	
	@Bean
	public RouterFunction<?> routes(RouteHandlers handlers) {
		return route(GET("/movies"), handlers::all)
				.andRoute(GET("/movies/{movieId}"), handlers::byId)
				.andRoute(GET("/movies/{movieId}/events"), handlers::events);
	}
}

@Component
class RouteHandlers {
	@Autowired
	private FluxFlixService fluxFlixService;
	
	public Mono<ServerResponse> byId(ServerRequest request) {
		String movieId = request.pathVariable("movieId");
		return ServerResponse.ok().body(fluxFlixService.byId(movieId), Movie.class);
	}
	
	
	public Mono<ServerResponse> all(ServerRequest request) {
		return ServerResponse.ok().body(fluxFlixService.all(), Movie.class);
	}
	
	public Mono<ServerResponse> events(ServerRequest request) {
		String movieId = request.pathVariable("movieId");
		return ServerResponse.ok()
				.contentType(MediaType.TEXT_EVENT_STREAM)
				.body(fluxFlixService.straemAsStreams(movieId), MovieEvent.class);
	}
}

@Service
class FluxFlixService {

	@Autowired
	private MovieRepository repo;

	public Flux<Movie> all() {
		return repo.findAll();
	}

	public Mono<Movie> byId(String id) {
		return repo.findById(id);
	}

	public Flux<MovieEvent> straemAsStreams(String movieId) {
		return repo.findById(movieId)
				.<MovieEvent> flatMapMany(
						movie -> {
							Flux<Long> interval = Flux.interval(Duration
									.ofSeconds(1));
							Flux<MovieEvent> events = Flux.fromStream(Stream
									.generate(() -> new MovieEvent(movie,
											new Date())));

							return Flux.zip(interval, events, (i, m) -> m);
						});
	}
}

@Data
@NoArgsConstructor
@AllArgsConstructor
class MovieEvent {
	private Movie movie;
	private Date when;
}

interface MovieRepository extends ReactiveMongoRepository<Movie, String> {
}

@Data
@AllArgsConstructor
@NoArgsConstructor
@Document
@ToString
class Movie {
	@Id
	private String id;
	private String title;
}