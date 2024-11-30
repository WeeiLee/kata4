package software.ulpgc.kata4.io;

import software.ulpgc.kata4.model.Movie;

public class TsvMovieDeserializer implements MovieDeserializer{
    @Override
    public Movie deserialize(String text) {
        return deserialize(text.split("\t"));
    }

    private Movie deserialize(String[] fields) {
        return new Movie(
                fields[0],
                fields[2],
                toInt(fields[5]),
                toInt(fields[7])
        );
    }

    private int toInt(String field) {
        try {
            return Integer.parseInt(field);
        } catch (NumberFormatException e) {
            return -1;
        }
    }
}
