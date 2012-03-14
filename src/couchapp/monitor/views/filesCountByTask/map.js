function(doc) {
	if(doc.task){
                var task_name = doc.task ;
                var task = doc.task.split("/");
                if (task.length > 1){
                        task_name = task[1]
                }

                emit([task_name], {"state": doc.state, "user": doc.user, "destination": doc.destination, "source": doc.source, "id": doc.lfn});
        }
}
