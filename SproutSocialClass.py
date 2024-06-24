import requests

API_BASE_URL = "https://api.sproutsocial.com"
API_VERSION = "v1.1"
CUSTOMER_ID = "123456"
API_TOKEN = ""  # Don't store this here in production


class SproutResponse:
    """
    Represents a response from the Sprout Social API.

    Attributes:
        data (dict): The main content of the response.
        paging (dict, optional): Information for pagination.
        error (str, optional): Error message, if any.
        request_id (str, optional): ID of the request for tracking.
        api_version (str, optional): Version of the API used.
        server_version (str, optional): Version of the server responding.
    """

    def __init__(self, data, paging=None, error=None, request_id=None, api_version=None, server_version=None):
        self.data = data
        self.paging = paging
        self.error = error
        self.request_id = request_id
        self.api_version = api_version
        self.server_version = server_version

    @classmethod
    def from_response(cls, response):

        request_id = response.headers.get("X-Sprout-Request-ID")
        api_version = response.headers.get("X-Sprout-API-Version")
        server_version = response.headers.get("X-Sprout-Server-Version")
        error = None

        if response.ok:
            response_json = response.json()
            data = response_json.get("data", {})
            paging = response_json.get("paging", None)
            error = response_json.get("error", None)

            if error:
                raise Exception(f"Error: {error}, Request ID: {request_id}")

            return cls(data, paging, error, request_id, api_version, server_version)

        raise Exception(f"Error: {error}, Request ID: {request_id}")


class SproutRequest:
    def __init__(self, **kwargs):
        self.api_token = kwargs.get("api_token", API_TOKEN)
        self.customer_id = kwargs.get("customer_id", CUSTOMER_ID)
        self.api_base_url = kwargs.get("api_base_url", API_BASE_URL)
        self.api_version = kwargs.get("api_version", API_VERSION)
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {self.api_token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )

    def make_request(self, method, endpoint, **kwargs):
        url = f"{self.api_base_url}/{endpoint}"
        response = self.session.request(method, url, **kwargs)
        return SproutResponse.from_response(response)


def check_has_more_pages(paging_object):
    if paging_object:
        return paging_object["current_page"] != paging_object["total_pages"]


class Sprout:
    def __init__(
            self, api_token=API_TOKEN, customer_id=CUSTOMER_ID, api_version=API_VERSION, api_base_url=API_BASE_URL
    ):
        self.sprout_request = SproutRequest(
            api_token=api_token, customer_id=customer_id, api_base_url=api_base_url, api_version=api_version
        )

    def __del__(self):
        self.session.close()

    def _request(self, method, endpoint, **kwargs):
        return self.sprout_request.make_request(method, f"{self.sprout_request.api_version}/{endpoint}", **kwargs)

    def set_customer_id(self, customer_id):
        self.sprout_request.customer_id = customer_id

    def get_customer_id(self):
        return self._request("GET", "metadata/client")

    def get_profiles(self):
        return self._request("GET", f"{self.customer_id}/metadata/customer")

    def get_customer_tags(self):
        return self._request("GET", f"{self.customer_id}/metadata/tags")

    def get_customer_groups(self):
        return self._request("GET", f"{self.customer_id}/metadata/groups")

    def get_customer_users(self):
        return self._request("GET", f"{self.customer_id}/metadata/users")

    def get_customer_topics(self):
        return self._request("GET", f"{self.customer_id}/metadata/topics")

    def get_analytics_profiles(self, filters, metrics, **kwargs):
        data = {"filters": filters, "metrics": metrics, **kwargs}
        return self._request("POST", f"{self.customer_id}/analytics/profiles", json=data)

    def get_post_analytics(self, filters, metrics, **kwargs):
        data = {"filters": filters, "metrics": metrics, **kwargs}
        return self._request("POST", f"{self.customer_id}/analytics/posts", json=data)

    def get_messages_analytics(self, filters, **kwargs):
        data = {"filters": filters, **kwargs}
        return self._request("POST", f"{self.customer_id}/messages", json=data)

    def query_topic_messages(self, topic_id, filters, fields, metrics, sort, timezone, limit, page):
        data = {
            "filters": filters,
            "fields": fields,
            "metrics": metrics,
            "sort": sort,
            "timezone": timezone,
            "limit": limit,
            "page": page,
        }
        return self._request("POST", f"{self.customer_id}/listening/topics/{topic_id}/messages", json=data)

    # def query_all_topic_messages(self, topic_id, filters, fields, metrics, sort, timezone, limit=100):
    #     all_messages = []
    #     page = 1
    #     has_more_pages = True

    #     while has_more_pages:
    #         response = self.query_topic_messages(topic_id, filters, fields, metrics, sort, timezone, limit, page)
    #         sprout_response = SproutResponse.from_response(response)

    #         if sprout_response.data:
    #             all_messages.extend(sprout_response.data)
    #             # If the length of data is less than limit, it means this is the last page
    #             has_more_pages = len(sprout_response.data) == limit
    #             page += 1
    #         else:
    #             has_more_pages = False

    #     return all_messages

    def query_topic_metrics(self, topic_id, filters, metrics, sort, timezone, limit, page):
        data = {
            "filters": filters,
            "metrics": metrics,
            "sort": sort,
            "timezone": timezone,
            "limit": limit,
            "page": page,
        }
        return self._request("POST", f"{self.customer_id}/listening/topics/{topic_id}/metrics", json=data)

    def create_publishing_post(self, post_data):
        return self._request("POST", f"{self.customer_id}/publishing/posts", json=post_data)

    def retrieve_publishing_post(self, publishing_post_id):
        return self._request("GET", f"{self.customer_id}/publishing/posts/{publishing_post_id}")

    def upload_media(self, media_data):
        return self._request("POST", f"{self.customer_id}/media/", files=media_data)

    def start_multipart_media_upload(self, media_data):
        return self._request("POST", f"{self.customer_id}/media/submission", json=media_data)

    def continue_multipart_media_upload(self, submission_id, part_number, media_data):
        return self._request(
            "POST", f"{self.customer_id}/media/submission/{submission_id}/part/{part_number}", files=media_data
        )

    def complete_multipart_media_upload(self, submission_id):
        return self._request("GET", f"{self.customer_id}/media/submission/{submission_id}")

    def get_all_profiles(self):
        data = []
        profiles = self.get_profiles()
        if profiles.error:
            raise Exception(f"Error: {profiles.error}")
        if profiles.data:
            for profile in profiles.data:
                data.append({
                    "network_type": profile["network_type"],
                    "name": profile["name"],
                    "native_name": profile["native_name"],
                    "native_id": profile["native_id"],
                })
        return data

    def get_all_owned_profiles_analytics(self, filters, metrics, **kwargs):
        data = []
        profile_analytics = self.get_analytics_profiles(filters=filters, metrics=metrics, **kwargs)
        if profile_analytics.error:
            raise Exception(f"Error: {profile_analytics.error}")

        has_more_pages = True

        if profile_analytics.data:
            while has_more_pages:
                data.append(profile_analytics.data)
                profile_analytics = self.get_analytics_profiles(filters=filters, metrics=metrics, **kwargs)
                has_more_pages = check_has_more_pages(profile_analytics.paging)
        else:
            raise Exception("No data found")

        return data

    def get_all_post_analytics(self, filters, metrics, **kwargs):
        data = []
        post_analytics = self.get_post_analytics(filters=filters, metrics=metrics, **kwargs)
        if post_analytics.error:
            raise Exception(f"Error: {post_analytics.error}")

        has_more_pages = True

        if post_analytics.data:
            while has_more_pages:
                data.append(post_analytics.data)
                post_analytics = self.get_post_analytics(filters=filters, metrics=metrics, **kwargs)
                has_more_pages = check_has_more_pages(post_analytics.paging)
        else:
            raise Exception("No data found")

        return data
