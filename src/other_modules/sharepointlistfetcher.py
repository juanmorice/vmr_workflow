
import os
from shareplum import Office365, Site
from shareplum.site import Version


class SharePointListFetcher:

    def __init__(self):
        self.username = os.getenv("SHAREPOINT_USERNAME")
        self.password = os.getenv("SHAREPOINT_PASSWORD")
        self.base_url = os.getenv("SHAREPOINT_BASE_URL")
        self.site_url = os.getenv("SHAREPOINT_SITE_URL")
        self.list_title = os.getenv("SHAREPOINT_LIST_TITLE")

    def get_list_items(self, fields):

        try:
            authcookie = Office365(
                self.base_url,
                username=self.username,
                password=self.password
            ).GetCookies()

            site = Site(
                self.site_url,
                version=Version.v365,
                authcookie=authcookie
            )

            sp_list = site.List(self.list_title)

            data = sp_list.GetListItems(fields=fields)

            return data

        except Exception as e:
            raise RuntimeError(f"Error retrieving SharePoint data: {e}")