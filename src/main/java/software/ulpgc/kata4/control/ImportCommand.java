package software.ulpgc.kata4.control;

import software.ulpgc.kata4.io.*;
import software.ulpgc.kata4.model.Movie;
import software.ulpgc.kata4.ui.ImportDialog;

import java.io.File;
import java.io.IOException;


public class ImportCommand implements Command{
    private final ImportDialog dialog;

    public ImportCommand(ImportDialog dialog) {
        this.dialog = dialog;
    }


    @Override
    public void execute() {
        try (MovieReader reader = new GzipFileMovieReader(inputFile(), new TsvMovieDeserializer());
             MovieWriter writer = DatabaseMovieWriter.open(outputFile())){
            doExecute(reader, writer);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void doExecute(MovieReader reader, MovieWriter writer) throws IOException {
        while (true) {
            Movie movie = reader.read();
            if (movie == null) break;
            writer.write(movie);
        }
    }

    private File outputFile() {
        return new File("movies.db");
    }

    private File inputFile() {
        return dialog.get();
    }
}
