package software.ulpgc.kata4.io;

import software.ulpgc.kata4.model.Movie;

import java.io.File;
import java.sql.*;
import java.util.List;

import static java.sql.Types.INTEGER;
import static java.sql.Types.NVARCHAR;

public class DatabaseMovieWriter implements MovieWriter{
    private final Connection connection;
    private final PreparedStatement insertPreparedStatement;

    public DatabaseMovieWriter(Connection connection) throws SQLException {
        this.connection = connection;
        createTable();
        stopAutoCommit();
        this.insertPreparedStatement=createInsertPreparedStatement();
    }

    private final static String PreparedInsertStatement= """
                INSERT INTO movies(id, title, year, duration)
                VALUES(?,?,?,?)
            """;
    private PreparedStatement createInsertPreparedStatement() throws SQLException {
        return this.connection.prepareStatement(PreparedInsertStatement);
    }

    private void stopAutoCommit() throws SQLException {
        this.connection.setAutoCommit(false);
    }

    private final static String CreateTableStatement= """
            CREATE TABLE IF NOT EXISTS movies(
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                year INTEGER,
                duration INTEGER
            )
            """;
    private void createTable() throws SQLException {
        this.connection.createStatement().execute(CreateTableStatement);
    }

    public DatabaseMovieWriter(String connection) throws SQLException {
        this(DriverManager.getConnection(connection));
    }

    public static DatabaseMovieWriter open(File file) throws SQLException {
        return new DatabaseMovieWriter("jdbc:sqlite:" + file.getAbsolutePath());
    }

    @Override
    public void write(Movie movie) {
        try {
            insertPreparedStatementOf(movie).execute();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private PreparedStatement insertPreparedStatementOf(Movie movie) throws SQLException {
        this.insertPreparedStatement.clearParameters();
        parametersOf(movie).forEach(this::define);
        return insertPreparedStatement;
    }

    private void define(Parameter parameter) {
        try {
            this.insertPreparedStatement.setObject(parameter.index, parameter.value);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws Exception {
        this.connection.commit();
        this.connection.close();
    }

    private record Parameter(int index, Object value, int type){}
    private List<Parameter> parametersOf(Movie movie){
        return List.of(
                new Parameter(1, movie.id(), NVARCHAR),
                new Parameter(2, movie.title(), NVARCHAR),
                new Parameter(3, movie.year() != -1 ? movie.year() : null, INTEGER),
                new Parameter(3, movie.duration() != -1 ? movie.duration() : null, INTEGER)
        );
    }
}
