function(keys, values, rereduce) {
  var output = {'published': 0, 'not_published': 0};
  if (rereduce) {
      for (var someValue in values) {
      output['published'] += values[someValue]['published'];
      output['not_published'] += values[someValue]['not_published'];
      }
  } else {
      for (var someValue in values) {
      	output[values[someValue]['publication_state']] += 1;
	}
      
    }
  return output;
}
