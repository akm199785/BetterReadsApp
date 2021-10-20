package com.springboot.betterreads;

import com.springboot.betterreads.author.Author;
import com.springboot.betterreads.author.AuthorRepository;
import com.springboot.betterreads.book.Book;
import com.springboot.betterreads.book.BookRepository;
import com.springboot.betterreads.connection.DataStaxAstraProperties;
import com.springboot.betterreads.user.BooksByUserRepository;
import com.springboot.betterreads.userbooks.UserBooksRepository;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SpringBootApplication
@ComponentScan({"com.springboot.betterreads.book", "com.springboot.betterreads.author", "com.springboot.betterreads.search",
    "com.springboot.betterreads.home", "com.springboot.betterreads.user", "com.springboot.betterreads.userbooks"})
@EnableConfigurationProperties(DataStaxAstraProperties.class)
@EnableCassandraRepositories(basePackageClasses = {AuthorRepository.class, BookRepository.class, UserBooksRepository.class, BooksByUserRepository.class})
public class BetterReadsApp {

    @Autowired
    AuthorRepository authorRepository;


    @Autowired
    BookRepository bookRepository;

    @Value("${datadump.location.author}")
    private String authorDumpLocation;

    @Value("${datadump.location.works}")
    private String worksDumpLocation;

    public static void main(String[] args) {
        SpringApplication.run(BetterReadsApp.class, args);
    }


    private void initAuthors() {
        // get path and store path in Path variable from authorDump location
        Path path = Paths.get(authorDumpLocation);
        // now read lines of stream from files
        try (Stream<String> lines = Files.lines(path)) {
            lines.skip(210).forEach(line -> {
                //read and parse the line

                String jsonString = line.substring(line.indexOf("{"));
                try {
                    JSONObject jsonObject = new JSONObject(jsonString);
                    // construct the author object
                    Author author = new Author();
                    author.setName(jsonObject.optString("name"));
                    author.setPersonalName((jsonObject.optString("personal_name")));
                    author.setId(jsonObject.optString("key").replace("/authors/", ""));
                    //persist using Repository
                    System.out.println("saving " + author.getPersonalName());
                    authorRepository.save(author);
                } catch (JSONException e) {
                    e.printStackTrace();
                }

            });

            System.out.println("DONE!!!!!!!!!!!!");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void initWorks() {
        DateTimeFormatter datePattern=DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
        Path path = Paths.get(worksDumpLocation);
        try (Stream<String> lines = Files.lines(path)) {
            lines.skip(4242).forEach(line -> {
                String jsonString = line.substring(line.indexOf("{"));
                try {
                    JSONObject jsonObject = new JSONObject(jsonString);
                    // Add book data from jsonObject

                    Book book = new Book();
                    book.setId(jsonObject.optString("key").replace("/works/", ""));
                    book.setName(jsonObject.optString("title"));
                    JSONObject description = jsonObject.optJSONObject("description");
                    if (description != null) {
                        book.setDescription(description.optString("value"));
                    }
                    JSONObject publishedObj=jsonObject.optJSONObject("created");
                    if(publishedObj!=null)
                    {
                        String dateStr=publishedObj.getString("value");
                        book.setPubliationDate(LocalDate.parse(dateStr,datePattern));
                    }
                    JSONArray coverJsonArray = jsonObject.optJSONArray("covers");
                    if (coverJsonArray != null) {

                        List<String> coversIds = new ArrayList<>();
                        for (int i = 0; i < coverJsonArray.length(); i++) {
                            coversIds.add(coverJsonArray.getString(i));
                        }
                        book.setCoverIds(coversIds);
                    }
                    JSONArray authorJsonArray = jsonObject.optJSONArray("authors");
                    if (authorJsonArray != null) {
                        List<String> authorsId = new ArrayList<>();
                        for (int i = 0; i < authorJsonArray.length(); i++) {
                            authorsId.add(authorJsonArray.getJSONObject(i).getJSONObject("author").getString("key").replace("/authors/", ""));

                        }
                        book.setAuthorIds(authorsId);
                        List<String> authorsName = authorsId.stream().map(id -> authorRepository.findById(id))
                            .map(optionalAuthor -> {
                                if (!optionalAuthor.isPresent()) {
                                    return "Unknown Author";
                                } else {
                                    return optionalAuthor.get().getName();
                                }
                            }).collect(Collectors.toList());
                        book.setAuthorNames(authorsName);

                        System.out.println("saving Book "+book.getName());
                        bookRepository.save(book);
                    }

                } catch (JSONException e) {
                    e.printStackTrace();
                }
                }

            );

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @PostConstruct
    public void start() {

        //initAuthors();
        //initWorks();

    }


    @Bean
    public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties dataStaxAstraProperties) {
        Path bundle = dataStaxAstraProperties.getSecureConnectBundle().toPath();
        return cqlSessionBuilder -> cqlSessionBuilder.withCloudSecureConnectBundle(bundle);
    }
}
