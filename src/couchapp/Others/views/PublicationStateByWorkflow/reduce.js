function (key, values, rereduce) {
	var output = {'not_published': 0 ,'publishing': 0 ,'publication_failed': 0, 'published': 0};
	if (rereduce) {
		for (var someValue in values) {
			output['not_published'] += values[someValue]['not_published'];
                        output['publishing'] += values[someValue]['publishing'];
			output['publication_failed'] += values[someValue]['publication_failed'];
			output['published'] += values[someValue]['published'];
		}
	}
	else {
		for (var someValue in values) {
			output[values[someValue]] += 1;
		}
	}
	return output;
}
