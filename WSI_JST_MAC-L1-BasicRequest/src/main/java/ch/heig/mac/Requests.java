package ch.heig.mac;

import java.util.List;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.QueryOptions;


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
                """
        );
        return result.rowsAs(JsonObject.class);
    }

    public List<JsonObject> topReviewers() {
        var result = ctx.query("""
                SELECT c.email, count(c._id) AS nbCommentaire
                FROM `mflix-sample`._default.comments AS c
                GROUP BY c.email
                ORDER BY nbCommentaire DESC
                LIMIT 10
                """
        );
        return result.rowsAs(JsonObject.class);
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
        var result = ctx.query("Select Distinct m.imdb.id, m.imdb.rating, m.`cast`\n" +
                "From `mflix-sample`._default.movies AS m\n" +
                "where is_number(m.imdb.rating) AND m.imdb.rating > 8 AND " + actor +
                " in m.`cast`"
        );
        return result.rowsAs(JsonObject.class);
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
        var result = ctx.query("""
                Select m._id , m.title
                From `mflix-sample`._default.movies AS m
                where array_count(directors) > 20
                """
        );
        return result.rowsAs(JsonObject.class);
    }

    public List<JsonObject> commentsOfDirector1(String director) {
        throw new UnsupportedOperationException("Not implemented, yet");
    }

    public List<JsonObject> commentsOfDirector2(String director) {
        throw new UnsupportedOperationException("Not implemented, yet");
    }

    // Returns the number of documents updated.
    public long removeEarlyProjection(String movieId) {
        var result = ctx.query("UPDATE `mflix-sample`._default.theaters\n" +
                "SET schedule = ARRAY s FOR s IN schedule\n" +
                "WHEN s.moveId != " + movieId + " OR s.hourBegin >= \"18:00:00\" END\n" +
                "WHERE "+ movieId + " WITHIN schedule;",
                QueryOptions.queryOptions()
                            .parameters(JsonObject.create())
                            .metrics(true)
        );
        return result.metaData().metrics().isPresent() ?
                result.metaData().metrics().get().mutationCount() : 0;
    }

    public List<JsonObject> nightMovies() {
        throw new UnsupportedOperationException("Not implemented, yet");
    }


}
