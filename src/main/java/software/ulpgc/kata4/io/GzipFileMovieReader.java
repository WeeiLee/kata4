package software.ulpgc.kata4.io;

import software.ulpgc.kata4.model.Movie;

import java.io.*;
import java.util.zip.GZIPInputStream;

public class GzipFileMovieReader implements MovieReader{
    private final MovieDeserializer deserializer;
    private final BufferedReader reader;

    public GzipFileMovieReader(File file, MovieDeserializer deserializer) throws IOException {
        this.deserializer = deserializer;
        this.reader = readerOf(file);
        skipHeader();
    }

    private void skipHeader() throws IOException {
        this.reader.readLine();
    }

    private BufferedReader readerOf(File file) throws IOException {
        return new BufferedReader(new InputStreamReader(getGzipInputStream(file)));
    }

    private static GZIPInputStream getGzipInputStream(File file) throws IOException {
        return new GZIPInputStream(new FileInputStream(file));
    }

    @Override
    public Movie read() throws IOException {
        return readOf(this.reader.readLine());
    }

    private Movie readOf(String line) {
        return line != null ? this.deserializer.deserialize(line) : null;
    }

    @Override
    public void close() throws Exception {
        this.reader.close();
    }
}
