package io.buildmack.magnificentmediadataloader;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.validation.annotation.Validated;

import connection.DataStaxAstraProperties;
import jakarta.annotation.PostConstruct;

import org.json.JSONArray;
import org.json.JSONObject;

@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class)
public class MagnificentMediaDataLoaderApplication {

	@Autowired AuthorRepository authorRepository;
	@Autowired BookRepository bookRepository;

	@Value("${datadump.location.author}")
	private String authorDumpLocation;

	@Value("${datadump.location.works}")
	private String worksDumpLocation;

	public static void main(String[] args) {
		SpringApplication.run(MagnificentMediaDataLoaderApplication.class, args);
	}

	private void initAuthors(){
		Path path = Paths.get(authorDumpLocation);
		try (Stream<String> lines = Files.lines(path)) {
			lines.limit(50).forEach(line -> {
				//Read and parse the line

				String jsonString = line.substring(line.indexOf("{"));
				try {
					JSONObject jsonObject = new JSONObject(jsonString);
					
				//Construct Author Object
				Author author = new Author();
				author.setName(jsonObject.optString("name"));
				author.setId(jsonObject.optString("key").replace("/authors/",""));
				
				//Perisis using Repository
				authorRepository.save(author);

				} catch (Exception e) {
					e.printStackTrace();
				}

			});


		} catch (Exception e) {
			e.printStackTrace();
		}
	}	

	private void initBooks(){
		Path path = Paths.get(worksDumpLocation);
		//DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
		try (Stream<String> lines = Files.lines(path)) {
			lines.limit(50).forEach(line -> {
				//Read and parse the line
				String jsonString = line.substring(line.indexOf("{"));
				try {
					JSONObject jsonObject = new JSONObject(jsonString);
					
				//Construct Book Object
				Book book = new Book();

				book.setId(jsonObject.optString("key").replace("/works/", ""));

				book.setName(jsonObject.optString("title"));
				
				JSONObject descriptionObj = jsonObject.optJSONObject("description");
				
				if(descriptionObj != null){
					book.setDescription(descriptionObj.optString("value"));
				}

				JSONObject publishedObj = jsonObject.optJSONObject("created");

				if(publishedObj != null){
				String dateStr = publishedObj.getString("value");
				LocalDate publishedDate = LocalDate.from(DateTimeFormatter.ISO_LOCAL_DATE_TIME.parse(dateStr));
				book.setPublishedDate(publishedDate);
				}
				

				JSONArray coversJSONArr = jsonObject.optJSONArray("covers");
				if (coversJSONArr != null) {
					List<String> coverIds = new ArrayList<>();
					for (int i = 0; i < coversJSONArr.length(); i++){
						coverIds.add(coversJSONArr.getString(i));
					}
					book.setCoverIds(coverIds);
				}
				
				
				JSONArray authorsJSONArr = jsonObject.optJSONArray("authors");
				List<String> authorIds = new ArrayList<>();
				if (authorsJSONArr != null) {
					for (int i = 0; i < authorsJSONArr.length(); i++){
						String authorId = authorsJSONArr.getJSONObject(i).getJSONObject("author").getString("key")
								.replace("/authors/", "");
						authorIds.add(authorId);
					}
				} book.setAuthorIds(authorIds);	
				List<String> authorNames = authorIds.stream()
								.map(authorId -> authorRepository.findById(authorId)).flatMap(Optional::stream)
								.map(Author::getName).collect(Collectors.toList());

					book.setAuthorNames(authorNames);	

				System.out.println(authorIds);
				System.out.println(authorNames);
					// Perisis using Repository
					bookRepository.save(book);
				
				} catch (Exception e) {
					e.printStackTrace();
				}
			});
		} catch (Exception e) {
				e.printStackTrace();
		}
	}

	
	@PostConstruct
	public void start() {
		initAuthors();
		initBooks();
	}

	@Bean
    public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties astraProperties) {
        Path bundle = astraProperties.getSecureConnectBundle().toPath();
        return builder -> builder.withCloudSecureConnectBundle(bundle);
    }
	

}
