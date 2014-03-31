function couchapp_load(scripts) {
  for (var i=0; i < scripts.length; i++) {
    document.write('<script src="'+scripts[i]+'"><\/script>')
  };
  document.write('<link href="vendor\/protovis\/tipsy\/tipsy.css" type="text\/css" rel="stylesheet"\/>')
};

couchapp_load([
  "vendor/protovis/protovis-d3.3.js",
  "vendor/protovis/tipsy/jquery.tipsy.js",
  "vendor/protovis/tipsy/tipsy.js"
]);
