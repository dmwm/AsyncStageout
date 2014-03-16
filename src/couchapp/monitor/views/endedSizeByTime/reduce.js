function(keys, values, rereduce) {

    var output = {'done': {'njobs': 0, 'size': 0}, 'failed': {'njobs': 0, 'size': 0}, 'killed': {'njobs': 0, 'size': 0}, 'resubmitted': {'njobs': 0, 'size': 0}};

    var _keys = ['done', 'failed', 'killed', 'resubmitted'];
    var _values = ['njobs','size'];

    if (rereduce) {
        for (var v in values) {
            var value = values[v];
            for (var _k in _keys) {
                var _key = _keys[_k];
                for (var _v in _values) {
                    var _value = _values[_v];
                    output[_key][_value] += value[_key][_value];
                }
            }
        }
    }
    else {
        for (var v in values) {
            var value = values[v];
            var state = value['state'];
            output[state]['njobs'] += 1;
            for (var _v in _values) {
                var _value = _values[_v];
                if (_value == 'njobs') continue;
                output[state][_value] += value[_value];
            }
        }
    }
    return output;
}
