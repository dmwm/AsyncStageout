function (doc,req) {
        var states = {'failed': parseInt(req.query.failed) , 'done': parseInt(req.query.done), 'acquired': parseInt(req.query.acquired), 'new': parseInt(req.query.new), 'total': parseInt(req.query.total)};
        var new_states = {}
        for (state in states) {
        	if (states[state]) {
        		new_states[state] = states[state]
        	}
	}
        doc.state = new_states;
        doc.last_update = parseInt(req.query.last_update);
        return [doc, "OK"];
}
