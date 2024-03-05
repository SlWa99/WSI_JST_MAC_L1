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
                SELECT m.imdb.id AS imdb_id, m.tomatoes.viewer.rating AS tomatoes_rating, m.imdb.rating AS imdb_rating
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
                SELECT c.email, count(c._id) AS cnt
                FROM `mflix-sample`._default.comments AS c
                GROUP BY c.email
                ORDER BY cnt DESC
                LIMIT 10;
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
        var result = ctx.query("Select Distinct m.imdb.id AS imdb_id, m.imdb.rating, m.`cast` " +
                "From `mflix-sample`._default.movies AS m " +
                "where is_number(m.imdb.rating) AND m.imdb.rating > 8 AND \"" + actor + "\" in m.`cast`;"
        );
        return result.rowsAs(JsonObject.class);
    }

    public List<JsonObject> plentifulDirectors() {
        var result = ctx.query("""
                SELECT director_name, COUNT(m._id) count_film
                FROM `mflix-sample`.`_default`.`movies` m
                UNNEST directors AS director_name
                GROUP BY director_name
                HAVING COUNT(m._id) > 30;
                """
        );
        return result.rowsAs(JsonObject.class);
    }

    public List<JsonObject> confusingMovies() {
        var result = ctx.query("""
                Select m._id AS movie_id, m.title
                From `mflix-sample`._default.movies AS m
                where array_count(directors) > 20;
                """
        );
        return result.rowsAs(JsonObject.class);
    }

    public List<JsonObject> commentsOfDirector1(String director) {
        // il a fallu faire : CREATE INDEX movie_id ON `mflix-sample`._default.comments(movie_id);
        // pour créer un index sur la table comments sur movie_id

        var result = ctx.query(
                "SELECT c.movie_id, c.text " +
                "FROM `mflix-sample`._default.movies m " +
                "JOIN `mflix-sample`._default.comments c ON c.movie_id = m._id " +
                "WHERE ANY d IN m.directors SATISFIES d = \"" + director + "\" END;"

        );

        return result.rowsAs(JsonObject.class);
    }

    public List<JsonObject> commentsOfDirector2(String director) {
        var result = ctx.query("SELECT c.movie_id, c.text " +
                        "FROM `mflix-sample`._default.comments c " +
                        "WHERE c.movie_id IN ( " +
                        "SELECT RAW _id " +
                        "FROM `mflix-sample`._default.movies " +
                        "WHERE ANY d IN directors SATISFIES d = \"" + director + "\" END);"
        );

        return result.rowsAs(JsonObject.class);
    }

    // Returns the number of documents updated.
    public long removeEarlyProjection(String movieId) {
        var result = ctx.query("UPDATE `mflix-sample`._default.theaters\n" +
                        "SET schedule = ARRAY s FOR s IN schedule\n" +
                        "WHEN s.moveId != \"" + movieId + "\" OR s.hourBegin >= \"18:00:00\" END\n" +
                        "WHERE \"" + movieId + "\" WITHIN schedule;",
                        QueryOptions.queryOptions()
                        .parameters(JsonObject.create())
                        .metrics(true)
        );
        return result.metaData().metrics().isPresent() ?
                result.metaData().metrics().get().mutationCount() : 0;
    }


    public List<JsonObject> nightMovies() {
        var result = ctx.query("""
                SELECT _id movie_id, title
                FROM `mflix-sample`.`_default`.`movies`
                WHERE _id IN (SELECT RAW sched.movieId
                              FROM `mflix-sample`.`_default`.`theaters`
                              UNNEST schedule AS sched
                              GROUP BY sched.movieId
                              HAVING MIN(sched.hourBegin) >= "18:00:00");
                """
        );
        return result.rowsAs(JsonObject.class);
    }
}
