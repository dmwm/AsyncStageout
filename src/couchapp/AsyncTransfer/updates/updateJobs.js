function (doc,req) {
	if (req.query.state != 'done'){
	        doc.retry_count.push(req.query.retry);
	} else {
		doc.lfn = req.query.lfn;
	}
        if (req.query.state == 'done'){
                doc.end_time = req.query.end_time;
        }
        if (req.query.state == 'failed'){
                doc.failure_reason = req.query.failure_reason;
                doc.end_time = req.query.end_time;
        }
        if (req.query.state == 'killed'){
                doc.end_time = req.query.end_time;
        }
	if (req.query.state == 'new'){
        	doc.end_time = req.query.end_time;
	}
	doc.last_update = parseInt(req.query.last_update)
        doc.state = req.query.state;
        return [doc, "OK"];

}
