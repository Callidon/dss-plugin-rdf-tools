var app = angular.module("dkurdftools.sparqlinput", []);

app.controller("SparqlInputFormController", function ($scope) {
  const yasgui = new Yasgui(document.getElementById("sparql_query_yasgui"));
  yasgui.on("change", (args) => {
    $scope.config.sparql_query = args.getQueryWithValues();
  });
});
