function (doc,req) {
        if (req.query.state != 'done'){
	        doc.retry_count.push(req.query.retry);
                if (req.query.state == 'failed'){
			doc.end_time = req.query.end_time;
	        }
	}
        doc.last_update = parseInt(req.query.last_update)
        doc.state = req.query.state;
        return [doc, "OK"];
}
