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

app.directive('historyChart', function() {
    function link(scope, element, attrs) {
        
        // Rickshaw doesn't work with an empty chart. So we have to initialise it as soon as we get real data.
        scope.graph = null;

        scope.$watch(attrs.historyChart, function(value) {
            // Data will be null first time.
            if (value != null && value.length > 0) {
                
                $("#chart_container", $(element)).show()

                if (scope.graph == null) {
                    scope.graph = new Rickshaw.Graph( {
                    element: document.querySelector("#chart", $(element)), 
                    width: 1170, 
                    height: 400,
                    renderer: 'bar',
                    series: [{
                        color: 'steelblue',
                        data: value
                        }]
                    });

                    var x_axis = new Rickshaw.Graph.Axis.Time({graph: scope.graph});

                    var y_axis = new Rickshaw.Graph.Axis.Y( {
                            graph: scope.graph,
                            orientation: 'left',
                            tickFormat: Rickshaw.Fixtures.Number.formatKMBT,
                            element: document.getElementById('y_axis'),
                    } );

                    scope.graph.render();
                } else {
                    scope.graph.series[0].data = value;
                    scope.graph.update();
                }
            } else {
                $("#hart_container", $(element)).hide()
            }

        });
    }    

    return {
        link: link,
        templateUrl : "partials/chart-template.html"
    };
})


// app.directive('myCurrentTime', function($interval, dateFilter) {
 
//     function link(scope, element, attrs) {
//       var format,
//           timeoutId;
 
//       function updateTime() {
//         element.text(dateFilter(new Date(), format));
//       }
 
//       scope.$watch(attrs.myCurrentTime, function(value) {
//         format = value;
//         updateTime();
//       });
 
//       element.on('$destroy', function() {
//         $interval.cancel(timeoutId);
//       });
 
//       // start the UI update process; save the timeoutId for canceling
//       timeoutId = $interval(function() {
//         updateTime(); // update DOM
//       }, 1000);
//     }
 
//     return {
//       link: link
//     };
//   });