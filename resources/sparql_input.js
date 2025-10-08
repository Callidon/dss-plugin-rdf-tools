var app = angular.module("dkurdftools.sparqlinput", []);

app.controller("SparqlInputFormController", function ($scope) {
  const yasgui = new Yasgui(document.getElementById("sparql_query_yasgui"));
  yasgui.on("change", (args) => {
    $scope.config.spaql_query = args.getQueryWithValues();
  });
});
