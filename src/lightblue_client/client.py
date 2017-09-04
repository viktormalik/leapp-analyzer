import json
import pycurl
import sys
from StringIO import StringIO
from system_blueprint.rpm_packages import RpmList


class LightblueQuery:
    def __init__(self):
        self.data = {
            "projection": list(),
            "query": {"$and": list()},
            "sort": list()
        }

    @staticmethod
    def _simple_value_query(field, op, rvalue):
        return {
            "field": field,
            "op": op,
            "rvalue": rvalue
        }

    @staticmethod
    def _simple_field_query(field, op, rfield):
        return {
            "field": field,
            "op": op,
            "rfield": rfield
        }

    @staticmethod
    def _array_value_query(field, op, values):
        return {
            "field": field,
            "op": op,
            "values": values
        }

    @staticmethod
    def _conjunction(list):
        return {"$and": list}

    @staticmethod
    def _array_match(array, match_query):
        return {
            "array": array,
            "elemMatch": match_query
        }

    @staticmethod
    def _simple_projection(field):
        return {
            "field": field,
            "include": True,
            "recursive": True
        }

    @staticmethod
    def _array_projection(field, match_list, projection_list, sort_list):
        return {
            "field": field,
            "include": True,
            "match": {
                "$and": match_list
            },
            "projection": projection_list,
            "sort": sort_list
        }

    def add_field_projection(self, field):
        self.data["projection"].append(
            self._simple_projection(field))

    def add_simple_value_query(self, field, op, value):
        self.data["query"]["$and"].append(
            self._simple_value_query(field, op, value))

    def add_array_value_query(self, field, array):
        self.data["query"]["$and"].append(
            self._array_value_query(field, "$in", array))

    def add_latest_image_projection(self, projection_fields, sort_fields=[]):
        self.data["projection"].append(self._array_projection(
            "images",
            [self._array_match(
                "repositories",
                self._conjunction(
                    [self._simple_field_query("repository",
                                              "=",
                                              "$parent.$parent.repository"),
                     self._simple_field_query("registry",
                                              "=",
                                              "$parent.$parent.registry"),
                     self._simple_value_query("published",
                                              "=",
                                              "true"),
                     # self._simple_value_query("tags.*.name",
                     #                          "=",
                     #                          "latest")
                     ]))],
            [self._simple_projection(f) for f in projection_fields],
            sort_fields))

    def add_rhcc_query(self):
        self.add_simple_value_query("registry", "=",
                                    "registry.access.redhat.com")
        self.add_simple_value_query("published", "=", True)
        self.add_simple_value_query("images.*.repositories.*.published", "=",
                                    True)

    # Query for a given repo in Red Hat Container Catalog
    def add_rhcc_repo_query(self, repo):
        self.add_rhcc_query()
        self.add_simple_value_query("repository", "=", repo)

    def json(self):
        return json.dumps(self.data)


# Serves for easy parsing of response returned from the Lightblue database
# Response is a list of entities matching the query sent in request.
# ResponseSelector allows to select inner JSON fields of entities in the list.
# Selected fields can be returned using project_on_selector, or can be used to
# filter the overall answer by using apply_selector_as_filter.
class LightblueResponse:
    def __init__(self, json_string):
        self.data = json.loads(json_string)
        self.count = self.data["matchCount"]
        self.results = self.data["processed"]
        self.selector = self.ResponseSelector()

    def new_selector(self):
        self.selector = self.ResponseSelector()

    def project_on_selector(self):
        return self.selector.project(self.results)

    def check_single_result(self):
        if self.count == 0:
            raise LightblueQueryException("No entities found")
        elif self.count > 1:
            raise LightblueQueryException(
                "Too many entities found for given name")

    def apply_selector_as_filter(self, op, value):
        self.results = self.selector.filter(self.results, op, value)

    class ResponseSelector:
        def __init__(self):
            self.fields = []

        def select_field(self, field):
            self.fields.append(field)

        def select_first(self):
            self.select_field(0)

        def _project_on_elem(self, elem, ignore_missing=True):
            filtered = elem
            for field in self.fields:
                if ignore_missing:
                    if field not in filtered:
                        return None
                filtered = filtered[field]
            return filtered

        def project(self, data):
            return [self._project_on_elem(elem, False) for elem in data]

        def filter(self, data, op, value):
            def _project_and_check(elem, op, value):
                proj_elem = self._project_on_elem(elem)
                return proj_elem and op(value, proj_elem)

            return [elem for elem in data if
                    _project_and_check(elem, op, value)]


class ContainerCatalog:
    BASE_URL = "https://lightbluedatasvc.dev.a1.vary.redhat.com/rest/data/find/"
    HEADERS = "Content-Type:application/json"

    # The certificate is not used for curl since it uses NSS database where
    # the certificate is stored. Using requests seems not to be possible, since
    # it is not able to query NSS database for certificates.
    CERT = "/home/vmalik/Documents/certs/lb-vmalik-exported.pem"

    def _get_url(self, entity):
        return self.BASE_URL + entity

    def _send_request(self, entity, query):
        print(self._get_url(entity))

        buffer = StringIO()
        c = pycurl.Curl()
        c.setopt(pycurl.URL, self._get_url(entity))
        c.setopt(pycurl.HTTPHEADER, [self.HEADERS])
        c.setopt(pycurl.POST, 1)
        c.setopt(pycurl.POSTFIELDS, query.json())
        c.setopt(pycurl.WRITEDATA, buffer)
        c.perform()

        if c.getinfo(pycurl.RESPONSE_CODE) != 200:
            raise LightblueConnectionException(c.getinfo(pycurl.RESPONSE_CODE),
                                               buffer.getvalue())

        body = buffer.getvalue()
        return LightblueResponse(body)

        # r = requests.post(self._get_url(entity), headers=self.HEADERS,
        #                 json=query.data, cert=self.CERT, verify=False)
        # if r.status_code != 200:
        #     print(r.status_code)
        #     print(r.text)
        #     raise LightblueConnectionException
        # print(r.json())

    # Gets repo by its name
    def get_repo(self, repo):
        q = LightblueQuery()
        q.add_field_projection("repository")
        q.add_field_projection("display_data.name")
        q.add_rhcc_repo_query(repo)

        self._send_request("containerRepository", q)

    # Gets image by its id
    def get_image(self, id):
        q = LightblueQuery()
        q.add_field_projection("repositories.*.repository")
        q.add_field_projection("docker_image_id")
        q.add_simple_value_query("_id", "=", id)

        response = self._send_request("containerImage", q)
        response.check_single_result()
        return response.results[0]

    # Gets list of RPM packages in the latest image of given repo
    def get_repo_rpms(self, repo):
        q = LightblueQuery()
        q.add_latest_image_projection([
            "parsed_data.rpm_manifest.*.name",
            "parsed_data.rpm_manifest.*.version",
            "parsed_data.rpm_manifest.*.architecture",
            "parsed_data.rpm_manifest.*.nvra"
        ])
        q.add_rhcc_repo_query(repo)

        response = self._send_request("containerRepository", q)
        response.selector.select_field("images")
        response.selector.select_first()
        response.selector.select_field("parsed_data")
        response.selector.select_field("rpm_manifest")

        response.check_single_result()
        result = RpmList()
        for rpm in response.project_on_selector()[0]:
            result.add(rpm)
        return result

    # Gets list of RPM packages in given image
    def get_image_rpms(self, id):
        q = LightblueQuery()
        q.add_field_projection("parsed_data.rpm_manifest")
        q.add_simple_value_query("_id", "=", id)

        response = self._send_request("containerImage", q)
        response.selector.select_field("parsed_data")
        response.selector.select_field("rpm_manifest")

        response.check_single_result()
        rpm_list = RpmList()
        for rpm in response.project_on_selector()[0]:
            rpm_list.add(rpm)
        return rpm_list

    # Searches for repositories with given openshift tags and return id of the
    # latest image in each such repository
    def get_images_with_openshift_tag(self, tag):
        q = LightblueQuery()
        q.add_field_projection("display_data.name")
        q.add_field_projection("display_data.openshift_tags")
        q.add_latest_image_projection([
            "_id",
            "repositories.*.tags.*.name"
        ], [{"repositories.*.tags.*.name": "$desc"}])
        q.add_rhcc_query()

        response = self._send_request("containerRepository", q)
        response.selector.select_field("display_data")
        response.selector.select_field("openshift_tags")
        response.apply_selector_as_filter(lambda x, y: x in y, tag)

        response.new_selector()
        response.selector.select_field("images")
        response.selector.select_first()
        response.selector.select_field("_id")
        return response.project_on_selector()


class LightblueConnectionException(Exception):
    def __init__(self, status_code, response_body):
        self.message = \
            "LightBlue database returned code {}\nResponse body:\n{}" \
                .format(status_code, response_body)
        sys.stderr.write(self.message + "\n")


class LightblueQueryException(Exception):
    def __init__(self, message):
        self.message = "Error querying LightBlue database:\n{}".format(message)
        sys.stderr.write(self.message + "\n")
