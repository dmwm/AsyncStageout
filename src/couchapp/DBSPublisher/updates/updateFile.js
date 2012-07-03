function (doc,req) {
        if (req.query.state != 'published'){
                doc.publication_retry_count.push(req.query.retry);
        }
        doc.last_update = parseInt(req.query.last_update)
        doc.publication_state = req.query.publication_state;
        return [doc, "OK"];
}
