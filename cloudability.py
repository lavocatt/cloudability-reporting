from datetime import datetime as dt, timedelta as td
from typing import Any, Dict, List, Optional
from urllib import parse
import requests

CLOUDABILITY_API = "api.cloudability.com"


class Measures:

    measures: List[Dict[str, str]] = []

    def __init__(self, measures: List[Dict[str, str]]) -> None:
        self.measures = measures

    def name_from_label(self, label: str) -> str:
        """
        Retrieves the name from the label in the measures
        """
        for measure in self.measures:
            if measure["label"] == label:
                return measure["name"]
        raise RuntimeError(f"Label {label} not found in measures.json")

    def type_from_name(self, name: str) -> str:
        """
        Retrieves the data_type from the name in the measures
        """
        for measure in self.measures:
            if measure["name"] == name:
                return measure["data_type"]
        raise RuntimeError(f"Name {name} not found in measures.json")


class Request:
    filters: List[str]
    dimensions: List[str]
    metrics: List[str]
    name_mapping: Optional[Dict[str, str]]
    days: int = 7

    def __init__(self,
                 filters: Optional[List[str]] = None, dimensions:
                 Optional[List[str]] = None, metrics: Optional[List[str]] =
                 None, mappings: Optional[Dict[str, str]] = None,
                 days=7) -> None:
        if filters:
            self.filters = filters
        if dimensions:
            self.dimensions = dimensions
        if metrics:
            self.metrics = metrics
        if mappings:
            self.name_mapping = mappings
        self.days = days

    def report(self, handler, measures: Measures):
        now = dt.now()
        start_date = (now - td(days=self.days)).strftime("%Y-%m-%d")
        end_date = now.strftime("%Y-%m-%d")
        list_report = handler(
            f"dimensions={','.join(self.dimensions)}"
            f"&start_date={start_date}"
            f"&end_date={end_date}"
            f"&filters={'&filters='.join(self.filters)}"
            f"&metrics={','.join(self.metrics)}")
        return self._request_result_to_dict(list_report, measures)

    def _request_result_to_dict(self, request_result, measures: Measures) -> Dict[str, List[str | float | int]]:
        """
        Transform the list of results from the report of the cloudability v3 api
        to a dict where each key is a metric/dimension and the value is a list
        of support
        values.
        User can get custom names for each key by providing a name_mapping dict.
        Where each key is the `name` of the measure and the value is the customized
        name to get as the key in the returned dict.
        """
        ret: Dict[str, List[str | float | int]] = {}
        types: Dict[str, str] = {}
        for metric in self.metrics:
            ret[metric] = []
            types[metric] = measures.type_from_name(metric)
        for dimension in self.dimensions:
            ret[dimension] = []
            types[dimension] = measures.type_from_name(dimension)
        for line in request_result:
            for metric in self.metrics:
                ret[metric].append(
                    Request.convert(types[metric], line[metric])
                )
            for dimension in self.dimensions:
                ret[dimension].append(
                    Request.convert(types[dimension], line[dimension])
                )
        # perform name mapping for the keys
        if self.name_mapping:
            mapped_ret = {}
            for k, v in ret.items():
                mapped_ret[self.name_mapping[k]] = v
            return mapped_ret
        return ret

    @staticmethod
    def make_filter(key: str, operator: str, value: str) -> str:
        """
        Creates a cloudability v3 valid filter. Checks that the filter is supported
        by the API and then convert the string using escape chars for each special
        ones.
        """
        # see https://help.apptio.com/en-us/cloudability/api/v3/cost_reporting_endpoints.htm
        filter_operators = [
            "!=@",  # does not contain
            "!=",   # not equals
            "<=",   # less than or equals
            "<",    # less than
            "=@",   # contains
            "[]!=",  # not in*
            "[]=",  # in*
            "==",   # equals
            ">",    # greater than
            "===",  # strictly equals*
            "!==",  # strictly not equals*
            ">="    # greater than or equals
        ]
        if operator not in filter_operators:
            raise RuntimeError("Unsupported operator")
        return parse.quote(f"{key}{operator}{value}")

    @staticmethod
    def convert(tp: str, value: Any):
        """
        Gets a type extracted from the measures and convert the value to this
        type
        """
        if tp == "float":
            return float(value)
        if tp == "date":
            return str(value)
        if tp == "percentage":
            return float(value)
        if tp == "integer":
            return int(value)
        if tp == "currency":
            return float(value)
        if tp == "string":
            return str(value)
        raise RuntimeError(f"Unsupported type {tp}")


class CloudabilityReport:

    cloudaility_token: str

    def __init__(self, cloudaility_token: str) -> None:
        self.cloudaility_token = cloudaility_token
        self.measures = Measures(self._query_measures())

    def run_request(self, request: Request):
        return request.report(self._query_report, self.measures)

    def _query_report(self, query: str) -> List:
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
        endpoint = "reporting/cost/run"
        request = requests.get(
            url=f"https://{CLOUDABILITY_API}/v3/{endpoint}?{query}",
            headers=headers,
            auth=(self.cloudaility_token, '')
        )
        if not isinstance(request.json(), Dict):
            raise RuntimeError(
                f"Report result should be dict {query} {request.json()}")
        if not request.json():
            raise RuntimeError("Was expecting response")
        if request.json()["pagination"]:
            RuntimeError("Pagination not yet supported")
        assert len(request.json()["results"]) == int(
            request.json()["total_results"]), "Pagination not supported yet"
        return request.json()["results"]

    def _query_measures(self) -> List:
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
        endpoint = "reporting/cost/measures"
        request = requests.get(
            url=f"https://{CLOUDABILITY_API}/v3/{endpoint}",
            headers=headers,
            auth=(self.cloudaility_token, '')
        )
        if not isinstance(request.json(), list):
            raise RuntimeError(
                f"Report result should be a list {request.json()}")
        return request.json()
