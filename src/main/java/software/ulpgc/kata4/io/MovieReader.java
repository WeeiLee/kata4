package software.ulpgc.kata4.io;

import software.ulpgc.kata4.model.Movie;

import java.io.IOException;

public interface MovieReader extends AutoCloseable{
    Movie read() throws IOException;
}
