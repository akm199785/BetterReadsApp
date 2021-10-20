package com.springboot.betterreads.userbooks;

import com.springboot.betterreads.book.Book;
import com.springboot.betterreads.book.BookRepository;
import com.springboot.betterreads.user.BooksByUser;
import com.springboot.betterreads.user.BooksByUserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.core.user.OAuth2User;
import org.springframework.stereotype.Controller;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.method.support.ModelAndViewContainer;
import org.springframework.web.servlet.ModelAndView;

import java.time.LocalDate;
import java.util.Optional;

@Controller
public class UserBooksController {
    @Autowired
    UserBooksRepository userBooksRepository;

    @Autowired
    BookRepository bookRepository;

    @Autowired
    BooksByUserRepository booksByUserRepository;
   public ModelAndView addBookForUser(@RequestBody MultiValueMap<String,String> formData,
                                      @AuthenticationPrincipal OAuth2User principal)
   {
       if(principal == null || principal.getAttribute("login")==null)
       {
           return  null;
       }
       String bookId=formData.getFirst("bookId");
       Optional<Book> optionalBook=bookRepository.findById(bookId);
       if(optionalBook.isPresent())
       {
           return new ModelAndView("redirect:/");
       }
       Book book=optionalBook.get();
       UserBooks userBooks=new UserBooks();
       UserBooksPrimaryKey key =new UserBooksPrimaryKey();
       String userId=principal.getAttribute("login");
       key.setBookId(bookId);
       key.setUserId(userId);
       userBooks.setKey(key);
       int rating=Integer.parseInt((formData.getFirst("rating")));
       userBooks.setStartedDate(LocalDate.parse(formData.getFirst("startDate")));
       userBooks.setCompletedDate(LocalDate.parse(formData.getFirst("completedDate")));
       userBooks.setRating(rating);
       userBooks.setReadingStatus(formData.getFirst("readingStatus"));

       userBooksRepository.save(userBooks);

       BooksByUser booksByUser = new BooksByUser();
       booksByUser.setId(userId);
       booksByUser.setBookId(bookId);
       booksByUser.setBookName(book.getName());
       booksByUser.setCoverIds(book.getCoverIds());
       booksByUser.setAuthorNames(book.getAuthorNames());
       booksByUser.setReadingStatus(formData.getFirst("readingStatus"));
       booksByUser.setRating(rating);

       booksByUserRepository.save(booksByUser);
       return new ModelAndView("redirect:/books/" + bookId);

   }
}
