package ch.heig.mac;

import java.util.List;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.json.JsonObject;


public class Requests {
    private final Cluster ctx;

    public Requests(Cluster cluster) {
        this.ctx = cluster;
    }

    public List<String> getCollectionNames() {
        var result = ctx.query("""
                        SELECT RAW r.name
                        FROM system:keyspaces r
                        WHERE r.`bucket` = "mflix-sample";
                        """
        );
        return result.rowsAs(String.class);
    }

    public List<JsonObject> inconsistentRating() {
        var result = ctx.query("""
                SELECT Distinct m.imdb.id, m.tomatoes.viewer.rating AS tomatoesRating, m.imdb.rating AS imdbRating
                FROM `mflix-sample`._default.movies AS m
                WHERE m.tomatoes.viewer.rating != 0 AND abs(m.imdb.rating - m.tomatoes.viewer.rating) > 7;
                """
        );
        return result.rowsAs(JsonObject.class);
    }

    public List<JsonObject> hiddenGem() {
        var result = ctx.query("""
                        SELECT m.title
                        FROM `mflix-sample`._default.movies m
                        WHERE m.tomatoes.critic.rating == 10
                        AND m.tomatoes.viewer.rating IS NOT VALUED;
                        throw new UnsupportedOperationException("Not implemented, yet");
                        """
        );
        return result.rowsAs(JsonObject.class);
    }

    public List<JsonObject> topReviewers() {
        throw new UnsupportedOperationException("Not implemented, yet");
    }

    public List<String> greatReviewers() {
        var result = ctx.query("""
                        SELECT RAW c.email
                        FROM `mflix-sample`._default.comments c
                        GROUP BY c.email
                        HAVING COUNT(c.email) > 300;
                        """
        );
        return result.rowsAs(String.class);
    }

    public List<JsonObject> bestMoviesOfActor(String actor) {
    throw new UnsupportedOperationException("Not implemented, yet");
    }

    public List<JsonObject> plentifulDirectors() {
        var result = ctx.query("""
                        SELECT m.directors[0] AS director_name, COUNT(m.title) AS count_film
                        FROM `mflix-sample`._default.movies m
                        GROUP BY m.directors
                        HAVING COUNT(m.directors) > 30;
                        """
        );
        return result.rowsAs(JsonObject.class);
    }

    public List<JsonObject> confusingMovies() {
    throw new UnsupportedOperationException("Not implemented, yet");
    }

    public List<JsonObject> commentsOfDirector1(String director) {
    throw new UnsupportedOperationException("Not implemented, yet");
    }

    public List<JsonObject> commentsOfDirector2(String director) {
    throw new UnsupportedOperationException("Not implemented, yet");
    }

    // Returns the number of documents updated.
    public long removeEarlyProjection(String movieId) {
    throw new UnsupportedOperationException("Not implemented, yet");
    }

    public List<JsonObject> nightMovies() {
    throw new UnsupportedOperationException("Not implemented, yet");
    }


}
