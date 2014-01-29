'use strict';

/* Controllers */

var app = angular.module('ology.controllers', []);



app.controller('TopDomainsController', ["$scope", "$http", "$resource", function($scope, $http, $resource) {
    var TopDomainsResource = $resource('/top-domains', {}, {'query': {method: 'GET', isArray: true}});

    $scope.startDate = "2010-01-01";
    $scope.endDate = "2014-01-01";
    $scope.pageNumber = 1;
    $scope.subdomainMethod = "none";

    $scope.fetch = function() {
        $scope.data = TopDomainsResource.query({"start-date": $scope.startDate, "end-date": $scope.endDate, "page": $scope.pageNumber, "subdomain-rollup": $scope.subdomainMethod});
    };

    // Reset page number when other values change.
    $scope.$watch("startDate", function() {
        $scope.pageNumber = 1;
        $scope.fetch();
    });

    $scope.$watch("endDate", function() {
        $scope.pageNumber = 1;
        $scope.fetch();
    });
    
    $scope.$watch("pageNumber", function() {
        $scope.fetch();
    });
    
    $scope.$watch("subdomainMethod", function() {
        $scope.pageNumber = 1;
        $scope.fetch();
    });
    
}]);

app.controller('HistoryController', ["$scope", "$http", "$resource", function($scope, $http, $resource) {
    var DaysResource = $resource('/days', {}, {'query': {method: 'GET', isArray: false}});

    $scope.startDate = "2010-01-01";
    $scope.endDate = "2014-01-01";
    $scope.groupMethod = "month";
    $scope.data = null;
    

    $scope.fetch = function() {
        DaysResource.query({"start-date": $scope.startDate, "end-date": $scope.endDate, "domain": $scope.domain, "doi": $scope.doi, "group-method": $scope.groupMethod}, function(response) {
            
            var days = response.result.days;

            // Turn into chart-friendly.
            $scope.resultErrors = null;
            
            // Empty result, no info.
            if (days.length > 0 ) {
                $scope.resultInfo = {"count": response.result.count, "startDate": new Date(days[0].date), "endDate": new Date(days[days.length-1].date)}
            } else {
                $scope.resultInfo = null;
            }

            $scope.data = days.map(function(point) { return({x: new Date(point.date).getTime()/1000, y: point.count});});
        }, function(response) {
            // Error
            $scope.resultErrors = response.data;
            $scope.resultInfo = null;
            $scope.data = null;
        });
    };
}]);

function getCorrelation(xArray, yArray) {
  function sum(m, v) {return m + v;}
  function sumSquares(m, v) {return m + v * v;}
  function filterNaN(m, v, i) {isNaN(v) ? null : m.push(i); return m;}

  // clean the data (because we know that some values are missing)
  var xNaN = _.reduce(xArray, filterNaN , []);
  var yNaN = _.reduce(yArray, filterNaN , []);
  var include = _.intersection(xNaN, yNaN);
  var fX = _.map(include, function(d) {return xArray[d];});
  var fY = _.map(include, function(d) {return yArray[d];});

  var sumX = _.reduce(fX, sum, 0);
  var sumY = _.reduce(fY, sum, 0);
  var sumX2 = _.reduce(fX, sumSquares, 0);
  var sumY2 = _.reduce(fY, sumSquares, 0);
  var sumXY = _.reduce(fX, function(m, v, i) {return m + v * fY[i];}, 0);

  var n = fX.length;
  var ntor = ( ( sumXY ) - ( sumX * sumY / n) );
  var dtorX = sumX2 - ( sumX * sumX / n);
  var dtorY = sumY2 - ( sumY * sumY / n);
 
  var r = ntor / (Math.sqrt( dtorX * dtorY )); // Pearson ( http://www.stat.wmich.edu/s216/book/node122.html )
  var m = ntor / dtorX; // y = mx + b
  var b = ( sumY - m * sumX ) / n;

  // console.log(r, m, b);
  return {r: r, m: m, b: b};
}

function mean(xs) {
    var total = 0;
    for (var i = 0; i < xs.length; i++) {
        total += xs[i];
    }

    return (total / xs.length);
}

app.directive('historyChart', function() {
    function link(scope, element, attrs) {

        // Start hidden, show with first data.
        $("#chart_container", $(element)).hide()
        $("#chart_container_extras", $(element)).show()
        
        // Rickshaw doesn't work with an empty chart. So we have to initialise it as soon as we get real data.
        scope.graph = null;

        scope.$watch("trend", function(value) {
            if (scope.graph) {
                if (value == true) {
                    scope.graph.series[1].enable()
                    scope.graph.series[0].renderer = "scatterplot";
                    scope.graph.setRenderer("multi");
                    scope.graph.update();

                } else {
                    scope.graph.series[1].disable()
                    scope.graph.series[0].renderer = "bar";

                    scope.graph.setRenderer("bar");
                    scope.graph.update();
                }
            }
        });

        scope.$watch(attrs.historyChart, function(value) {
            // Data will be null first time.
            if (value != null && value.length > 0) {

                var ys = [];
                var xs = [];

                // First remove aberrations from incomplete processing. 
                for (var i = 0; i < value.length; i++) {
                    ys.push(value[i].y);
                }

                var averageY = mean(ys);
                var aberrationMin = averageY / 100;

                ys = [];
                for (var i = 0; i < value.length; i++) {
                    if (value[i].y > aberrationMin) {
                        xs.push(value[i].x);
                        ys.push(value[i].y);
                    }
                }

                // var r = linearRegression(ys, xs);
                var r = getCorrelation(xs, ys);

                var trendLine = [];
                for (var i = 0; i < value.length; i++) {
                    var x = value[i].x;
                    var y = (r.m * x) + r.b;
                    trendLine.push({x: x, y: y})
                }

                $("#chart_container", $(element)).show()
                $("#chart_container_extras", $(element)).show()

                if (scope.graph == null) {
                    scope.graph = new Rickshaw.Graph( {
                    element: document.querySelector("#chart", $(element)),
                    width: 1100, 
                    height: 400,
                    renderer: 'multi',
                    series: [
                        {color: 'steelblue', data: value, renderer: "scatterplot", name: "Count" },
                        {color: 'red', data: trendLine, renderer: "line", name: "Trend"}
                    ]});

                    var x_axis = new Rickshaw.Graph.Axis.Time({graph: scope.graph});

                    var y_axis = new Rickshaw.Graph.Axis.Y( {
                            graph: scope.graph,
                            orientation: 'left',
                            tickFormat: Rickshaw.Fixtures.Number.formatKMBT,
                            element: document.getElementById('y_axis'),
                    } );

                    var detail = new Rickshaw.Graph.HoverDetail({
                        graph: scope.graph
                    });

                    var shelving = new Rickshaw.Graph.Behavior.Series.Toggle( {
                        graph: scope.graph
                    } );

                    scope.graph.render();

                    // Start by setting trend display off, trigger change event above.
                    scope.trend = false;
                } else {
                    scope.graph.series[0].data = value;
                    scope.graph.series[1].data = trendLine;
                    scope.graph.update();
                }
            } else {
                $("#chart_container", $(element)).hide()
                $("#chart_container_extras", $(element)).hide()
            }

        });
    }    

    return {
        link: link,
        templateUrl : "partials/chart-template.html"
    };
})
