import urllib.parse
from typing import Dict, List
import requests

class Parley(object):
    """the class that communicates with the UK Parliament API
    houses the methods to fetch data from the API's various endpoints
    houses some methods to transform the data into a more usable format

    fetch makes a single call and returns the response

    paginated fetch makes use of the linked page.next url to paginate
    results, returning the first pages response body but with all items 
    from all pages

    exhaustive fetch makes use of the Skip and Take query parameters to manually
    paginate through results ending only when a pages items is less than the
    Take parameter TODO: change this to use totalResults so that it doesn't
    infinitely request the final page when it matches the Take parameter

    the clean type method takes a type endpoints listed results and converts it
    to a dictionary with the id as the key and the rest of the data as the value
    """
    def __init__(
            self,
            base_endpoint: str,
            page_size_max: int = 20
        ):

        self.session = requests.Session()
        self.base_endpoint = base_endpoint
        self.page_size_max = page_size_max

    def __fetch__(self, endpoint: str, **kwargs):
        if len(kwargs)>0:
            endpoint += "?" + urllib.parse.urlencode(kwargs)
        res = self.session.get(self.base_endpoint + endpoint)
        res.raise_for_status()
        return res.json()

    def __paginated_fetch__(self, endpoint: str, **kwargs):
        response = self.__fetch__(endpoint, **kwargs)
        next_page_url = ''
        for link in response["links"]:
            if link["rel"] == "page.next":
                next_page_url = link["href"]
                break
        if len(response["items"]) < self.page_size_max:
            return response
        response["items"].extend(
            self.__paginated_fetch__(next_page_url)["items"]
        )
        return response
    
    def __exhaustive_fetch__(self, endpoint: str, **kwargs):
        skip_pages = kwargs.pop("Skip", 0)
        take_pages = kwargs.pop("Take", self.page_size_max)
        response = self.__fetch__(endpoint, Skip=skip_pages, Take=take_pages, **kwargs)
        if len(response["items"]) < take_pages:
            return response
        skip_pages += take_pages
        response["items"].extend(
            self.__exhaustive_fetch__(
                endpoint,
                Skip=skip_pages,
                Take=take_pages,
                **kwargs
            )["items"]
        )
        return response
    
class Bills(object):
    """class to orchastrate Parley fetching bill data
    """
    ENDPOINT = {
        "base": "https://bills-api.parliament.uk/api/",
        "search": "v1/Bills",
        "sittings": "v1/Sittings"
    }
    PAGE_SIZE_MAX = 999
    def __init__(self):
        self.parley = Parley(
            base_endpoint=self.ENDPOINT["base"],
            page_size_max=self.PAGE_SIZE_MAX
        )
        self.bill_types = self.__clean_types__("v1/BillTypes")
        self.publication_types = self.__clean_types__("v1/PublicationTypes")
        self.stage_types = self.__clean_types__("v1/Stages")
    

    def get_bills(self, **kwargs):
        response = self.parley.__exhaustive_fetch__(
            self.ENDPOINT["search"],
            **kwargs
        )
        bills = []
        for bill_summary in response["items"]:
            bill_id = bill_summary["billId"]
            bill = self.parley.__fetch__(
                endpoint=self.ENDPOINT["search"]+"/"+str(bill_id)
            )
            bills.append(bill)
        return bills
    
    def get_publications(self, bill_id: int):
        return self.parley.__fetch__(
            self.ENDPOINT["search"] \
                + "/" + str(bill_id) \
                + "/Publications"
        )["publications"]

    def get_amendments(self, bill_id: int, stage_id: int):
        return self.parley.__exhaustive_fetch__(
            self.ENDPOINT["search"] \
            + "/" + str(bill_id) \
            + "/Stages" \
            + "/" + str(stage_id) \
            + "/Amendments"
        )["items"]

    def get_stages(self, bill_id: int):
        bill_stages = self.parley.__exhaustive_fetch__(
            self.ENDPOINT["search"]+"/"+str(bill_id)+"/Stages"
        )["items"]
        for stage in bill_stages:
            stage_id = stage["id"]
            stage["documents"] = self.parley.__fetch__(
                self.ENDPOINT["search"] \
                + "/" + str(bill_id) \
                + "/Stages" \
                + "/" + str(stage_id) \
                + "/Publications"
            )
        return bill_stages
    
    def get_sittings(self, start_date: str, end_date: str):
        # gets the date of the bills are being discussed
        return self.parley.__exhaustive_fetch__(
            self.ENDPOINT["sittings"],
            dateFrom=start_date,
            dateTo=end_date
        )

    def __clean_types__(self, endpoint: str):
        raw_types = self.parley.__exhaustive_fetch__(endpoint)["items"]
        return {t["id"]:{k:v for k,v in t.items() if k!="id"} for t in raw_types}
        

class Members(object):
    ENDPOINT = {
        "base": "https://members-api.parliament.uk/api/",
        "search": "Members/Search"
    }
    PAGE_SIZE_MAX = 20

    def __init__(self):
        self.parley = Parley(
            base_endpoint=self.ENDPOINT["base"],
            page_size_max=self.PAGE_SIZE_MAX
        )
    
    def get_members(self) -> List[Dict]:
        """api docs: https://arc.net/l/quote/cqvmmikl
        gets all members ever

        Returns:
            List[Dict]: json list of member dictionary objects
        """
        # TODO: make members detailed getting linked data
        raw_data = self.parley.__paginated_fetch__(self.ENDPOINT["search"])["items"]
        return [member["value"] for member in raw_data]

class Divisions(object):
    COMMONS_ENDPOINT = {
        "base": "https://commonsvotes-api.parliament.uk/data/",
        "search": "divisions.json/search",
        "division": "division/{division_id}.json"
    }
    LORDS_ENDPOINT = {
        "base": "https://lordsvotes-api.parliament.uk/data/",
        "search": "Divisions/search"
    }
    PAGE_SIZE_MAX = 999
    
    def __init__(self, house: str):
        self.house = house
        if self.house.lower() == "commons":
            self.ENDPOINT = self.COMMONS_ENDPOINT
        elif self.house.lower() == "lords":
            self.ENDPOINT = self.LORDS_ENDPOINT
        else:
            raise ValueError("house must be 'Commons' or 'Lords'")
        self.parley = Parley(
            base_endpoint=self.ENDPOINT["base"],
            page_size_max=self.PAGE_SIZE_MAX
        )
    
    def get_divisions(self, start_date: str, end_date: str):
        # TODO: commons is yyyy-mm-dd, so needs a rearange func
        response = self.parley.__fetch__(
            self.ENDPOINT["search"],
            startDate=start_date,
            endDate=end_date,
            includeWhenMemberWasTeller=True
        )

        if self.house.lower()=="lords":
            return response
        
        for division in response:
            division_id = division["DivisionId"]
            detailed_division = self.parley.__fetch__(
                self.ENDPOINT["division"].format(division_id=division_id)
            )
            division["Ayes"]=detailed_division["Ayes"]
            division["Noes"]=detailed_division["Noes"]
        
        return response
        

class Calendar(object):
    # https://whatson-api.parliament.uk/swagger/ui/index#!/Events/Events_EventsByDate
    # requires date dd-mm-yyyy to get all events that date

    # https://whatson-api.parliament.uk/swagger/ui/index#!/Events/Events_EventsByDate
    # ^ not linked with rest of the documentation
    ENDPOINT = {
        "base": "https://whatson-api.parliament.uk/calendar/",
        "events": "events/list.json",
        "non_sitting_days": "events/nonsitting.json",
        "sessions": "sessions/list.json",
        "speakers": "events/speakers.json"
    }
    PAGE_SIZE_MAX = 31

    def __init__(self):
        self.parley = Parley(
            base_endpoint=self.ENDPOINT["base"],
            page_size_max=self.PAGE_SIZE_MAX
        )

    def get_references(self):
        event_types = self.__clean_types__("types/list.json")
        event_categories = self.__clean_types__("categories/list.json")
        event_locations = self.__clean_types__("locations/list.json")
        return {
            "event_types": event_types,
            "event_categories": event_categories,
            "event_locations": event_locations
        }
    
    def get_sessions(self):
        # always gets all of them (only way i think)
        return self.parley.__fetch__(
            self.ENDPOINT["sessions"]
        )
    
    def get_events(self, start_date: str, end_date: str):
        # dd-mm-yyyy
        return self.parley.__fetch__(
            self.ENDPOINT["events"],
            startDate=start_date,
            endDate=end_date
        )
    
    def get_recesses(self, start_date: str, end_date: str):
        return self.parley.__fetch__(
            self.ENDPOINT["non_sitting_days"],
            startDate=start_date,
            endDate=end_date
        )
    
    def __clean_types__(self, endpoint: str):
        raw_types = self.parley.__fetch__(endpoint)
        return {t["Id"]:{k:v for k,v in t.items() if k!="Id"} for t in raw_types}

