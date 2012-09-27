function (key, values, rereduce) {
	var output = {'done': 0, 'failed': 0, 'acquired':0, 'new': 0};
	if (rereduce) {
		for (var someValue in values) {
			output['new'] += values[someValue]['new'];
			output['done'] += values[someValue]['done'];
			output['failed'] += values[someValue]['failed'];
                        output['acquired'] += values[someValue]['acquired'];
		}
	} 
	else {
		for (var someValue in values) {
			output[values[someValue]] += 1;
		}
	}
	return output;
}
