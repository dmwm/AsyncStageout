function(keys, values, rereduce) {
  var output = {'new': 0, 'acquired': 0};
  if (rereduce) {
      for (var someValue in values) {
      output['new'] += values[someValue]['new'];
      output['acquired'] += values[someValue]['acquired'];
    }
  } else {
      for (var someValue in values) {
      	output[values[someValue]['state']] += 1;
	}
      
    }
  return output;
}