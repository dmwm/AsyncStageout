
function fullpieActive(data, canvas, labels) {

  var w = 500,
      h = 350,
      r = h / 2,
      a = pv.Scale.linear(0, pv.sum(data)).range(0, 2 * Math.PI);

  var vis = new pv.Panel()
    .width(w)
    .height(h)
    .def("s", -1)
    .canvas(canvas || 'pie')

  .add(pv.Wedge)
    .data(data.sort(pv.reverseOrder))
    .outerRadius(r-20)
    .angle(a)
    .left(function() r + Math.cos(this.startAngle() + this.angle() / 2)* ((this.parent.s() == this.index) ? 10 : 0))
    .bottom(function() r - Math.sin(this.startAngle() + this.angle() / 2) * ((this.parent.s() == this.index) ? 10 : 0))
    .event("mouseover", function() this.parent.s(this.index))
    .anchor("center").add(pv.Label)
      .visible(function(d) {return (d > .15)})
      .font("12px sans-serif")
      .text(function(d) {
        if (labels) {
          return labels[this.index];
        } else {
          return d.toFixed(2);
        }
      })
    .anchor("top").add(pv.Label)
       .text(function(d) {
        if (labels) {
          return data[this.index];
        } else {
          return d.toFixed(2);
        }
        });

  vis.add(pv.Label)
    .left(370)
    .top(20)
    .textAlign("center")
    .font("12px sans-serif")
    .text("Selection Info:");



 vis.render();

};

function fullpieActiveTest(canvas, input) {

  var w = 800,
      h = 500,
      r = h / 2,
      a = pv.Scale.linear(0, pv.sum(input.data)).range(0, 2 * Math.PI);



  var vis = new pv.Panel()
    .width(w)
    .height(h)
    .def("s", -1)
    .canvas(canvas || 'pie')

    .add(pv.Wedge)
    	.data(input.data)
    	.outerRadius(r-20)
    	.angle(a)
    	.left(function() r + Math.cos(this.startAngle() + this.angle() / 2)* ((this.parent.s() == this.index) ? 10 : 0))
    	.bottom(function() r - Math.sin(this.startAngle() + this.angle() / 2) * ((this.parent.s() == this.index) ? 10 : 0))
    	.event("mouseover", function() this.parent.s(this.index))
        .event("click", function() self.location = input.url+"?index="+input.labels[this.index])
    	.anchor("center").add(pv.Label)
      		.visible(function(d) {return ((d > .15)&&(this.parent.s() != this.index))})
      		.font("13px sans-serif")
      		.text(function(d) {
        if (input.labels) {
          return input.labels[this.index];
        } else {
          return d.toFixed(2);
        }
      })
    	.anchor("center").add(pv.Label)
        	.visible(function(d) {return ((d > .15)&&(this.parent.s() == this.index))})
       		.text(function(d) d.toFixed(0));

  vis.add(pv.Label)
    .left(600)
    .top(20)
    .textAlign("center")
    .font("bold 12px sans-serif")
    .text(function() this.parent.s() > -1 ? "Selected - "+input.labels[this.parent.s()]+": "+input.data[this.parent.s()]+" files" : "No Selected");

    vis.add(pv.Label)
    .left(600)
    .top(40)
    .textAlign("center")
    .font("bold 12px sans-serif")
    .text(function() this.parent.s() > -1 ? input.info[this.parent.s()] : "");


 vis.render();

};

function fullpieActive2(canvas, input) {

  var w = 800,
      h = 350,
      r = h / 2,
      a = pv.Scale.linear(0, pv.sum(input.data)).range(0, 2 * Math.PI);



  var vis = new pv.Panel()
    .width(w)
    .height(h)
    .def("s", -1)
    .canvas(canvas || 'pie')

    .add(pv.Wedge)
    	.data(input.data)
    	.outerRadius(r-20)
    	.angle(a)
    	.left(function() r + Math.cos(this.startAngle() + this.angle() / 2)* ((this.parent.s() == this.index) ? 10 : 0))
    	.bottom(function() r - Math.sin(this.startAngle() + this.angle() / 2) * ((this.parent.s() == this.index) ? 10 : 0))
    	.event("mouseover", function() this.parent.s(this.index))
        .event("click", function() self.location = input.url+"?index="+input.labels[this.index])
    	.anchor("center").add(pv.Label)
      		.visible(function(d) {return ((d > .15)&&(this.parent.s() != this.index))})
      		.text(function(d) d.toFixed(0))
    	.anchor("center").add(pv.Label)
        	.visible(function(d) {return ((d > .15)&&(this.parent.s() == this.index))})
       		.text(function(d) d.toFixed(0));

  vis.add(pv.Label)
    .left(500)
    .top(20)
    .textAlign("center")
    .font("bold 12px sans-serif")
    .text(function() this.parent.s() > -1 ? "Selected  "+input.info[this.parent.s()]: " ");



 vis.render();

};

function fullpieActiveLegend(canvas, input) {

  var w = 900,
      h = 500,
      r = h / 2,
      a = pv.Scale.linear(0, pv.sum(input.data)).range(0, 2 * Math.PI);

  var root = new pv.Panel()
    .width(w)
    .height(h)
    .canvas(canvas || 'pie');

  var vis = root.add(pv.Panel)
    .width(h)
    .height(h)
    .left(0)
    .bottom(0)
    .def("s", -1)
    .add(pv.Wedge)
    	.data(input.data)
	.fillStyle(pv.Colors.category20().by(pv.index))
    	.outerRadius(r-40)
    	.angle(a)
    	.left(function() r + Math.cos(this.startAngle() + this.angle() / 2)* ((this.parent.s() == this.index) ? 10 : 0))
    	.bottom(function() r - Math.sin(this.startAngle() + this.angle() / 2) * ((this.parent.s() == this.index) ? 10 : 0))
    	.event("mouseover", function() this.parent.s(this.index))
        .event("click", function() self.location = input.url+"?index="+input.labels[this.index])
    	.anchor("center").add(pv.Label)
      		.visible(function(d) {return d > .15})
      		.font("bold 13px sans-serif")
		.textAngle(0)
      		.text(function(d) d.toFixed(0));

    root.add(pv.Dot)
	.data(input.data)
	.right(300)
	.top(function(d) 20 + this.index * 20)
        .shapeRadius(8)
	.fillStyle(pv.Colors.category20().by(pv.index))
        .shape("square")
        .lineWidth(1)
	.anchor("right").add(pv.Label)
        .font("bold 13px sans-serif")
	.text(function(d) input.labels[this.index]);


 root.render();

};


function drawpie(canvas, input) {

var w = 400,
      h = 400,
      r = w / 2;

  var vis = new pv.Panel()
      .canvas(canvas)
      .def("s", -1)
      .width(w)
      .height(w);

  vis.add(pv.Wedge)
      .data(input.data)
      .event("mouseover", function() this.parent.s(this.index))
      .event("mouseout", function() this.parent.s(-1))
      .bottom(w / 2)
      .left(w / 2)
      .innerRadius(0)
      .outerRadius(r)
      .angle(pv.Scale.linear(0, pv.sum(input.data)).range(0, 2 * Math.PI))

      .anchor("center").add(pv.Label)
      	.visible(function(d) {return ((d > .15)&&(this.parent.s() != this.index))})
      	.font("13px sans-serif")
      	.text(function(d) {
       	 if (input.labels) {
          return input.labels[this.index];
       	 } else {
          return d.toFixed(2);
        	}
      	})

      .anchor("top").add(pv.Label)
        	.visible(function(d) {return ((d > .15)&&(this.parent.s() == this.index))})
                .font("13px sans-serif")
       		.text(function(d) {
        	if (true) {
          	return d.toFixed(0);
        	} else {
          	return d.toFixed(0);
        	}
        	});

  vis.render();
};


function simpleActivePie(canvas, input) {

 var w = 350,
      h = 300,
      r = h / 2,
      textfont = "13px sans-serif",
      fill = pv.colors("#2ca02c", "#d62728","#e7ba52"),
      selected = pv.colors("#2ca02c", "#d62728","#e7ba52");

 var root = new pv.Panel()
      .canvas(canvas)
      .width(w)
      .height(w+40);

 var vis = root.add(pv.Panel)
      .def("s", -1)
      .top(20)
      .width(w)
      .height(w);

  vis.add(pv.Wedge)
      .data(input.totdata)
      .fillStyle(fill.by(pv.index))
      .event("mouseover", function() this.parent.s(this.index))
      .event("mouseout", function() this.parent.s(-1))
      .bottom(w / 2)
      .left(w / 2)
      .innerRadius(0)
      .outerRadius(r-40)
      .angle(pv.Scale.linear(0, pv.sum(input.totdata)).range(0, 2 * Math.PI))
      .anchor("center").add(pv.Label)
      	.visible(function(d) {return ((d > .15)&&(this.parent.s() != this.index))})
      	.font(textfont)
      	.text(function(d) {
       	 if (input.status) {
          return input.status[this.index];
       	 } else {
          return d.toFixed(2);
        	}
      	})
      .anchor("top").add(pv.Label)
        	.visible(function(d) {return ((d > .15)&&(this.parent.s() == this.index))})
                .font(textfont)
       		.text(function(d) {
        	if (input.status) {
          	return d.toFixed(0);
        	} else {
          	return d.toFixed(0);
        	}
        	});

  root.add(pv.Dot)
	.data(input.status)
	.left(function(d) 60 + this.index * 100)
        .shapeRadius(8)
	.top(10)
        .shape("square")
        .fillStyle(fill.by(pv.index))
        .lineWidth(1)
	.anchor("right").add(pv.Label)
        .font(textfont)
	.text(function(d) d);

  root.render();
};

function stackedBarH(canvas, input) {

var tr = clone(input.stacked);
tr = pv.transpose(tr);
tr = tr.map(function(d){return d.reduce(function(a, b){ return a + b; }) } );

var w = 700,
    h = 25*(tr.length),
    fill = pv.colors("#b5cf6b", "#d6616b", "#000000",  "#e7ba52", "#6495ed", "#006400"),
    selected = pv.colors("#2ca02c", "#d62728","#000000",  "#e7ba52", "#6495ed", "#006400"),
    x = pv.Scale.linear(0, pv.max(tr)).range(0, w),
    y = pv.Scale.ordinal(pv.range(tr.length)).splitBanded(0, h, 6/7);


var root = new pv.Panel()
   .canvas(canvas)
   .width(w+200)
   .height(h+100);

var vis = root.add(pv.Panel)
    .canvas(canvas)
    .width(w)
    .height(h)
    .left(200)
    .bottom(20)
    .top(80);

var layout = vis.add(pv.Layout.Stack)
    .def("i", -1)
    .layers(input.stacked)
    .orient("left-top")
    .x(function() {return y(this.index);})
    .y(x)
var bar = layout.layer.add(pv.Bar)
    .height(y.range().band)
    .fillStyle(selected.by(pv.parent))
    .event("click", function(d) self.location = input.url+"?index="+input.labels[this.index])
    .event("mouseover", function() layout.i(this.index))
    .strokeStyle(function() layout.i() == this.index ? "black" : (this.fillStyle()));

//Legends
root.add(pv.Dot)
	.data(input.status)
	.left(function(d) 260 + this.index * 100)
        .shapeRadius(8)
	.top(40)
        .shape("square")
        .fillStyle(selected.by(pv.index))
        .lineWidth(1)
	.anchor("right").add(pv.Label)
        .font("bold 13px sans-serif")
	.text(function(d) d);

bar.anchor("left").add(pv.Label)
    .visible(function() !this.parent.index)
    .textMargin(5)
    .textAlign("right")
    .text(function() input.labels[this.index]);

vis.add(pv.Rule)
    .data(x.ticks())
    .left(x)
    .strokeStyle(function(d) d ? "rgba(255,255,255,.3)" : "#000")
  .add(pv.Rule)
    .bottom(0)
    .height(5)
    .strokeStyle("#000")
  .anchor("bottom").add(pv.Label)
    .text(function(d) d.toFixed(0));

bar.anchor("right").add(pv.Label)
    .visible(function(d) d > 0)
    .textStyle("white")
    .text(function(d) d.toFixed(0));

root.add(pv.Label)
  .font("bold 14px sans-serif")
  .top(75)
  .left(w/1.75)
  .text("Number of Files");


root.render();

};

function stackedBarHorizontal(canvas, input) {

var data = pv.range(3).map(function() pv.range(10).map(Math.random)),
    w = 400,
    h = 250,
    x = pv.Scale.linear(0, 3).range(0, w),
    y = pv.Scale.ordinal(pv.range(10)).splitBanded(0, h, 4/5);

var vis = new pv.Panel()
    .canvas(canvas)
    .width(w)
    .height(h)
    .bottom(20)
    .left(20)
    .right(10)
    .top(5);

var bar = vis.add(pv.Layout.Stack)
    .layers(data)
    .orient("left-top")
    .x(function() y(this.index))
    .y(x)
  .layer.add(pv.Bar)
    .height(y.range().band);

bar.anchor("right").add(pv.Label)
    .visible(function(d) d > .2)
    .textStyle("white")
    .text(function(d) d.toFixed(1));

bar.anchor("left").add(pv.Label)
    .visible(function() !this.parent.index)
    .textMargin(5)
    .textAlign("right")
    .text(function() "ABCDEFGHIJK".charAt(this.index));

vis.add(pv.Rule)
    .data(x.ticks())
    .left(x)
    .strokeStyle(function(d) d ? "rgba(255,255,255,.3)" : "#000")
  .add(pv.Rule)
    .bottom(0)
    .height(5)
    .strokeStyle("#000")
  .anchor("bottom").add(pv.Label)
    .text(function(d) d.toFixed(1));

vis.render();



}

function doublepie(canvas, input){


var w = 600,
    h = 450,
    p = w / 3,
    r = p / 2,
    ir = r-30,
    textfont = "13px sans-serif",
    fill = pv.colors("#2ca02c", "#d62728","#e7ba52"),
    a = pv.Scale.linear(0, pv.sum(input.dataop)).range(0, 2 * Math.PI);

var vis = new pv.Panel()
      .def("s", -1)
      .def("so", -1)
      .canvas(canvas || 'pie')
      .width(w)
      .height(h);

var extpie = vis.add(pv.Wedge)
      .data(input.dataop)
      .bottom(h / 2)
      .left(w / 2)
      .innerRadius(ir)
      .outerRadius(r+15)
      .angle(a)
      .event("mouseover", function() {return vis.so(this.index)})
      .event("click", function() update(this.index))
	.anchor("center").add(pv.Label)
      .visible(function(d) {return (d > 0)})
      .font(textfont)
      .textAngle(0)
      .text(function(d) {
        if (input.status) {
          return d.toFixed(0);
        } else {
          return d.toFixed(0);
        }
      });

var ipdata = [];
var ia = pv.Scale.linear(0, pv.sum([])).range(0, 2 * Math.PI);

var innerpie = vis.add(pv.Wedge)
      .data(function() {return ipdata;})
      .fillStyle(fill.by(pv.index))
      .visible(true)
      .bottom(h / 2)
      .left(w / 2)
      .innerRadius(0)
      .outerRadius(ir-5)
      .angle(function(d) {return ia(d);})
      .anchor("center").add(pv.Label)
      .visible(function(d) {return (d > 0)})
      .font(textfont)
      .text(function(d) {
        if (input.status) {
          return d.toFixed(0);
        } else {
          return d.toFixed(0);
        }
      });

function update(i) {
  ipdata = input.dataip[i];
  ia.domain(0, pv.sum(input.dataip[i]));
  vis.render();
}


  vis.add(pv.Label)
    .left(w/3)
    .top(90)
    .font("13px sans-serif")
    .text(function() this.parent.so() > -1 ? "SELECTED:  "+input.keys[this.parent.so()] : " ");

  vis.add(pv.Label)
	.left(w/3)
	.top(20)
	.font("13px sans-serif")
	.text("INNER PIE LEGEND");

  vis.add(pv.Dot)
	.data(input.status)
	.left(function(d) (w/3 + this.index * 100))
        .shapeRadius(8)
	.top(40)
        .shape("square")
        .fillStyle(fill.by(pv.index))
        .lineWidth(1)
	.anchor("right").add(pv.Label)
        .font("13px sans-serif")
	.text(function(d) d);

  vis.render();

}

function stackedArea(canvas, input, yaxislabel) {


var tr = clone(input.stacked);
tr = pv.transpose(tr);
tr = tr.map(function(d){return d.reduce(function(a, b){ return a.value + b.value; }) } );

var w = 850,
    h = 200,
    x = pv.Scale.ordinal(input.stacked[0], function(d) d.time).splitFlush(0, w),
    y = pv.Scale.linear(0, pv.max(tr)).range(0, h*.8);


var fill = pv.colors("#2ca02c", "#d62728", "#3299cc", "#ffff00");
var colors = {done: "#2ca02c", failed: "#d62728", new: "#3299cc", acquired:"#ffff00", killed:"#2ca02c", resubmitted:"#3299cc"};


/* The root panel. */
var root = new pv.Panel()
    .canvas(canvas)
    .width(w+80)
    .height(h+70);

var vis = root.add(pv.Panel)
    .width(w)
    .height(h)
    .bottom(90)
    .left(40)
    .right(40)
    .top(5);

/* X-axis and ticks. */
vis.add(pv.Rule)
    .data(input.stacked[0])
    .visible(function(d) d)
    .left(function(d) {return x(d.time);})
    .bottom(-5)
    .height(5)
  .anchor("bottom").add(pv.Label)
    .visible(function() this.index%(tr.length/25)<1 || tr.length <= 25)
    .textAlign("left")
    .textAngle(.25 * Math.PI)
    .text(function(d) d.time);

/* Y-axis and ticks. */
vis.add(pv.Rule)
    .data(y.ticks(5))
    .bottom(y)
    .strokeStyle(function(d) d ? "rgba(128,128,128,.2)" : "#000")
  .anchor("left").add(pv.Label)
    .text(y.tickFormat);


var area = vis.add(pv.Layout.Stack)
    .layers(input.stacked)
    .x(function(d) x(d.time))
    .y(function(d) y(d.value))
  .layer.add(pv.Area)
    .fillStyle(function () {return colors[input.status[this.parent.index]];})
    .anchor("top").add(pv.Line)
      .strokeStyle("black")
      .lineWidth(0)
    .add(pv.Dot)
      .shapeRadius(0)
    .add(pv.Rule)
    .height(function() this.proto.bottom() - 5)
    .lineWidth(1)
    .bottom(0);

root.add(pv.Label)
  .font("bold 13px sans-serif")
  .bottom(h/3)
  .left(15)
  .textAngle(1.5 * Math.PI)
  .text(yaxislabel)

root.add(pv.Label)
  .font("bold 13px sans-serif")
  .bottom(0)
  .left(w/2)
  .text("UTC Time")

root.add(pv.Dot)
	.data(input.status)
	.left(function(d) 260 + this.index * 100)
        .shapeRadius(8)
	.top(20)
        .shape("square")
        .fillStyle(function () {return colors[input.status[this.index]];})
        .lineWidth(1)
	.anchor("right").add(pv.Label)
        .font("bold 13px sans-serif")
	.text(function(d) d);

root.render();


};

function areaT(canvas, data) {

var w = 400,
    h = 200,
    x = pv.Scale.ordinal(data, function(d) d.ora).splitFlush(0, w),
    y = pv.Scale.linear(0, pv.max(data, function(d) d.value)).range(0, h);

/* The root panel. */
var vis = new pv.Panel()
    .width(w)
    .height(h)
    .bottom(20)
    .left(20)
    .right(10)
    .top(5);

/* X-axis and ticks. */
vis.add(pv.Rule)
    .data(data)
    .visible(function(d) d)
    .left(function(d) {return x(d.ora);})
    .bottom(-5)
    .height(5)
  .anchor("bottom").add(pv.Label)
    .text(function(d) d.ora);

/* Y-axis and ticks. */
vis.add(pv.Rule)
    .data(y.ticks(3))
    .bottom(y)
    .strokeStyle(function(d) d ? "rgba(128,128,128,.2)" : "#000")
  .anchor("left").add(pv.Label)
    .text(y.tickFormat);

	/* The area with top line. */
	vis.add(pv.Area)
	    .data(data)
	    .bottom(1)
	    .left(function(d) {return x(d.ora);})
	    .height(function(d) {return y(d.value);})
	    .fillStyle("rgb(121,173,210)")
	  .anchor("top").add(pv.Line)
	    .lineWidth(3);

	vis.render();
};


function matrix(canvas, input){

	Array.prototype.unique =  function() {
    var a = [];
    var l = this.length;
    for(var i=0; i<l; i++) {
      for(var j=i+1; j<l; j++) {
        // If this[i] is found later in the array
        if (this[i] === this[j])
          j = ++i;
      }
      a.push(this[i]);
    }
    return a;
  };

var source = clone(input.data);
source = source.map(function(d) {return d.key[1]} );
source = source.unique();


var dest = clone(input.data);
dest = dest.map(function(d) {return d.key[0]} );
dest = dest.unique();



	/* Sizing and scales. */
var w = 30*dest.length,
    h = 30*source.length,
    y = pv.Scale.ordinal(pv.range(source.length)).splitBanded(0, h),
    x = pv.Scale.ordinal(pv.range(dest.length)).splitBanded(0, w),
    dim = pv.Scale.linear(1, pv.max(input.data, function(d) d.value.total)).range(1, (pv.min([w/dest.length, h/source.length]))/2),
    c = pv.Scale.linear(0, 1).range('red', 'green');

var lg =    new pv.Panel()
    .width(340)
    .height(20)
    .top(50)
    .left(150)
  .add(pv.Bar)
    .data(pv.range(0, 1, 1/85))
    .left(function() this.index * 4)
    .width(4)
    .fillStyle(pv.Scale.linear(0, .5, 1).range('red', 'yellow', 'green'));


var dimmax = pv.min([(w/dest.length), (h/source.length)]);
var valuemax = pv.max(input.data, function(d) d.value.total);

/* The root panel. */
var vis = new pv.Panel()
	.canvas(canvas)
    .width(w)
    .height(h)
    .bottom(150)
    .left(150)
    .right(150)
    .top(80);

    var lg = vis.add(pv.Panel)
    .left(-150)
    .height(20)
    .top(-45)
  .add(pv.Bar)
    .data(pv.range(0, 1, 1/85))
    .left(function() this.index * 4)
    .width(4)
    .fillStyle(pv.Scale.linear(0, .5, 1).range('red', 'yellow', 'green'));

    /*
    lg.anchor("left").add(pv.Label)
		.font("13px bold sans-serif")
		.text("0%");

	lg.anchor("right").add(pv.Label)
		.font("13px bold sans-serif")
		.text("100%");
	 */

    vis.add(pv.Panel)
    .left(-150)
    .height(30)
    .top(-70)
    .anchor("left").add(pv.Label)
		.font("12px sans-serif")
		.text("Dot color range according to link efficiency from 0% to 100%");

var dot = vis.add(pv.Dot)
    .data(input.data)
    .left(function(d) x(d.key[0]))
    .bottom(function(d) y(d.key[1]))
    .event("click", function() self.location = input.url+"?index="+input.data[this.index].key)
    .shapeRadius(function(d) dim(d.value.total))
    .fillStyle(function(d) c(d.value.done/(d.value.done+d.value.failed)))
    .lineWidth(3)
    .strokeStyle(function(d) c(d.value.done/(d.value.done+d.value.failed)));

    dot.anchor("left").add(pv.Label)
    .left(-dimmax-20)
    .text(function(d) (d.key[1]));

vis.add(pv.Panel)
    .left(w/3)
    .height(30)
    .bottom(-dimmax-10)
    .anchor("left").add(pv.Label)
		.font("12px sans-serif")
		.text("Destination Site");


vis.add(pv.Rule)
    .data(dest)
    .visible(function(d) d)
    .left(function(d) {return x(d);})
    .bottom(-dimmax-20)
    .height(5)
  .anchor("bottom").add(pv.Label)
    .textAlign("left")
    .textAngle(.25 * Math.PI)
    .text(function(d) d);

    vis.add(pv.Label)
    .left(-dimmax)
    .textAngle(1.5 * Math.PI)
    .bottom(h/2)
    .font("12px sans-serif")
	.text("Source Site");

    vis.add(pv.Rule)
    .data(source)
    .left(-dimmax-20);


/*
vis.add(pv.Rule)
    .data(y.ticks())
    .bottom(y)
    .strokeStyle(function(d) d ? "#eee" : "#000")
  .anchor("left").add(pv.Label)
    .visible(function(d) d > 0 && d < 1)
    .text(y.tickFormat);

vis.add(pv.Rule)
    .data(x.ticks())
    .left(x)
    .strokeStyle(function(d) d ? "#eee" : "#000")
  .anchor("bottom").add(pv.Label)
    .visible(function(d) d > 0 && d < 100)
    .text(x.tickFormat);

vis.add(pv.Panel)
    .data(data)
  .add(pv.Dot)
    .left(function(d) x(d.x))
    .bottom(function(d) y(d.y))
    .strokeStyle(function(d) c(d.z))
    .fillStyle(function() this.strokeStyle().alpha(.2))
    .size(function(d) d.z)
    .title(function(d) d.z.toFixed(1));
*/
vis.render();


	}

function clone(o) {
 return eval(uneval(o));
}


