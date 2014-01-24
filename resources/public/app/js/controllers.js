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

    $scope.$watch("startDate", $scope.fetch);
    $scope.$watch("endDate", $scope.fetch);
    $scope.$watch("pageNumber", $scope.fetch);
    $scope.$watch("subdomainMethod", $scope.fetch);
}]);


app.controller('HistoryController', [function() {

  }]);