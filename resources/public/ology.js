// var app = angular.module('ology', ['ngAnimate', 'ngSanitize', 'mgcrea.ngStrap', 'mgcrea.ngStrap.datepicker', 'mgcrea.ngStrap.tooltip']);

// app.config(function($datepickerProvider) {
//   angular.extend($datepickerProvider.defaults, {
//     dateFormat: 'yyyy-MM-dd',
//     weekStart: 1
//   });
// })

// function DisplayCtrl($scope, $datepicker) {
//   $scope.todos = [
//     {text:'learn angular', done:true},
//     {text:'build an angular app', done:false}];
 
//   $scope.addTodo = function() {
//     $scope.todos.push({text:$scope.todoText, done:false});
//     $scope.todoText = '';
//   };
 
//   $scope.remaining = function() {
//     var count = 0;
//     angular.forEach($scope.todos, function(todo) {
//       count += todo.done ? 0 : 1;
//     });
//     return count;
//   };
 
//   $scope.archive = function() {
//     var oldTodos = $scope.todos;
//     $scope.todos = [];
//     angular.forEach(oldTodos, function(todo) {
//       if (!todo.done) $scope.todos.push(todo);
//     });
//   };

//     $scope.fromDate = "2014-01-10T00:00:00.000Z"; // <- [object Date]
//     $scope.untilDate = "2014-01-23T00:00:00.000Z"; // <- [object Date]

//   $scope.response = "!!"
// }



var app = angular.module('ology', ['ngAnimate', 'ngSanitize', 'mgcrea.ngStrap']);

app.controller('MainCtrl', function($scope) {
});

'use strict';

angular.module('ology')

.config(function($datepickerProvider) {
  angular.extend($datepickerProvider.defaults, {
    dateFormat: 'dd/MM/yyyy',
    weekStart: 1
  });
})

.controller('DisplayCrtl', function($scope, $http) {

  // $scope.selectedDate = new Date();
  // $scope.selectedDateNumber = Date.UTC(1986, 1, 22);
  // $scope.getType = function(key) {
  //   return Object.prototype.toString.call($scope[key]);
  // };

    $scope.fromDate = "2014-01-10T00:00:00.000Z"; // <- [object Date]
    $scope.untilDate = "2014-01-23T00:00:00.000Z"; // <- [object Date]


});

