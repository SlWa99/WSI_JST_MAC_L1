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
        throw new UnsupportedOperationException("Not implemented, yet");
    }

    public List<JsonObject> hiddenGem() {
        throw new UnsupportedOperationException("Not implemented, yet");
    }

    public List<JsonObject> topReviewers() {
        throw new UnsupportedOperationException("Not implemented, yet");
    }

    public List<String> greatReviewers() {
        throw new UnsupportedOperationException("Not implemented, yet");
    }

    public List<JsonObject> bestMoviesOfActor(String actor) {
        throw new UnsupportedOperationException("Not implemented, yet");
    }

    public List<JsonObject> plentifulDirectors() {
        throw new UnsupportedOperationException("Not implemented, yet");
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
