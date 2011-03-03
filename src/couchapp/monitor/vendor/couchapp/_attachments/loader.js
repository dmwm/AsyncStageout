
function couchapp_load(scripts) {
  for (var i=0; i < scripts.length; i++) {
    document.write('<script src="'+scripts[i]+'"><\/script>')
  };
  document.write('<link href="vendor\/protovis\/tipsy\/tipsy.css" type="text\/css" rel="stylesheet"\/>')
};

couchapp_load([
  "/_utils/script/sha1.js",
  "/_utils/script/json2.js",
  "/_utils/script/jquery.js",
  "/_utils/script/jquery.couch.js",
  "vendor/couchapp/jquery.couch.app.js",
  "vendor/couchapp/jquery.couch.app.util.js",
  "vendor/couchapp/jquery.mustache.js",
  "vendor/couchapp/jquery.evently.js",
  "vendor/protovis/protovis-d3.3.js",
  "vendor/protovis/tipsy/jquery.tipsy.js",
  "vendor/protovis/tipsy/tipsy.js",
  "vendor/plots/plots.js"
]);
