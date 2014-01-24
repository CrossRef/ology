'use strict';


// Declare app level module which depends on filters, and services
var app = angular.module('ology', [
  'ngRoute',
  'ngResource',
  'ology.filters',
  'ology.services',
  'ology.directives',
  'ology.controllers',
  'mgcrea.ngStrap'
])

app.config(['$routeProvider', function($routeProvider) {
  $routeProvider.when('/top-domains', {templateUrl: 'partials/top-domains.html', controller: 'TopDomainsController'});
  $routeProvider.when('/history', {templateUrl: 'partials/history.html', controller: 'HistoryController'});
  $routeProvider.otherwise({redirectTo: '/top-domains'});
}]);

app.config(function($datepickerProvider) {
  angular.extend($datepickerProvider.defaults, {
    dateFormat: 'yyyy-MM-dd',
    weekStart: 1
  });
})