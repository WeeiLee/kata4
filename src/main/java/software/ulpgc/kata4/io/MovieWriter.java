package software.ulpgc.kata4.io;

import software.ulpgc.kata4.model.Movie;

public interface MovieWriter extends AutoCloseable{
    void write(Movie movie);
}
