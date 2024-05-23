import urllib.parse
from typing import Dict, List, Union
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
    
class Committees(object):

    ENDPOINT = {
        "base": "https://committees-api.parliament.uk/api/",
        "committees": "Committees",
        "committee_business": "CommitteeBusiness",
        "committee_members": "Committees/{committee_id}/Members",
        "committee_attendance": "Committees/{committee_id}/"
    }
    PAGE_SIZE_MAX = 30
    def __init__(self):
        self.parley = Parley(
            base_endpoint=self.ENDPOINT["base"],
            page_size_max=self.PAGE_SIZE_MAX
        )
    
    def get_committees(self, **kwargs) -> List[dict]:
        return self.parley.__exhaustive_fetch__(self.ENDPOINT["committees"], **kwargs)["items"]
    
    def get_committee_business(self, committee_ids: Union[int,List[int]]) -> List[dict]:
        if isinstance(committee_ids, int):
            committee_ids = [committee_ids]
        committee_business = []
        for committee_id in committee_ids:
            business = self.parley.__exhaustive_fetch__(
                self.ENDPOINT["committee_business"],
                CommitteeId=committee_id
            )["items"]
            for b in business:
                b.update({"committeeId":committee_id})
            committee_business.extend(business)
        return committee_business
    
    def get_committee_members(self, committee_ids: Union[int, List[int]]) -> List[dict]:
        if isinstance(committee_ids, int):
            committee_ids = [committee_ids]
        committee_members = []
        for committee_id in committee_ids:
            members = self.parley.__exhaustive_fetch__(
                self.ENDPOINT["committee_members"].format(committee_id=committee_id)
            )["items"]
            for m in members:
                m.update({"committeeId":committee_id})
            committee_members.extend(members)
    
class Bills(object):
    """class to orchastrate ParleyPy fetching from the bill api
    """
    ENDPOINT = {
        "base": "https://bills-api.parliament.uk/api/",
        "bills": "v1/Bills",
        "sittings": "v1/Sittings",
        "stages": "v1/Bills/{bill_id}/Stages",
        "amendments": "v1/Bills/{bill_id}/Stages/{stage_id}/Amendments"
    }
    PAGE_SIZE_MAX = 999
    def __init__(self):
        self.parley = Parley(
            base_endpoint=self.ENDPOINT["base"],
            page_size_max=self.PAGE_SIZE_MAX
        )
    

    def get_bills(self, date: str = None, **kwargs):
        """gets all bills & there details for a parlimentry session (or session of provided date)
        required:
            Session: int (session id of session to fecth) OR date: str (date within session to fetch)
        """
        if not kwargs:
            kwargs = {}
        if date is not None:
            session_id=Calendar().get_session_id_for_date(date)
            kwargs["Session"]=session_id
        if "Session" not in kwargs:
            raise ValueError("`Session` or `date` must be provided")
        
        response = self.parley.__exhaustive_fetch__(
            self.ENDPOINT["bills"],
            **kwargs
        )
        bills = []
        for bill_summary in response["items"]:
            bill_id = bill_summary["billId"]
            bill = self.parley.__fetch__(
                endpoint=self.ENDPOINT["bills"]+"/"+str(bill_id)
            )
            bills.append(bill)
        return bills
    
    def get_stages(self, bill_ids: Union[int, List[int]]) -> List[dict]:
        if isinstance(bill_ids, int):
            bill_ids = [bill_ids]
        stages = []
        for bill_id in bill_ids:
            bill_stages = self.parley.__exhaustive_fetch__(self.ENDPOINT["stages"].format(bill_id=bill_id))["items"]
            for stage in bill_stages:
                stage.update({"billId":bill_id})
            stages.extend(bill_stages)
        return stages
    
    def get_amendments(self, bill_id: int, stage_ids: Union[int, List[int]]) -> List[dict]:
        if isinstance(stage_ids, int):
            stage_ids = [stage_ids]
        amendments = []
        for stage_id in stage_ids:
            stage_amendments = self.parley.__exhaustive_fetch__(
                self.ENDPOINT["amendments"].format(bill_id=bill_id,stage_id=stage_id)
            )["items"]
            amendments.extend(stage_amendments)
        return amendments
        

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
        "speakers": "events/speakers.json",
        "sessions_for_date": "sessions/forDate.json/{date}"
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
    
    def get_session_id_for_date(self, date: str) -> int:
        """queries the sessions api to find the session id for a given date, useful for endpoints that require a session id for filtering by date"""
        response = self.parley.__fetch__(self.ENDPOINT["sessions_for_date"].format(date=date))
        return response["SessionId"]
    
    def get_next_session_id(self, date: str = None, session_id: int = None) -> int:
        if date is None and session_id is None:
            raise ValueError("date or session_id must be provided")
        all_sessions = self.get_sessions()

    
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


class ParliamentReferences(object):
    # TODO: move this logic into parliament_ingestion and have Bills & Committees fetch their own types amalgamated by parliament_ingestion

    COMMITTEE_ENDPOINT = {
        "base": "https://committees-api.parliament.uk/api/",
        "committee": "CommitteeType",
        "committee_business": "CommitteeBusinessType"
    }
    BILL_ENDPOINT = {
        "base": "https://bills-api.parliament.uk/api/v1/",
        "bill": "BillTypes",
        "bill_publication": "PublicationTypes",
        "bill_stage": "Stages"
    }

    def __init__(self):
        self.bill_parley = Parley(base_endpoint=self.BILL_ENDPOINT["base"])
        self.committee_parley = Parley(base_endpoint=self.COMMITTEE_ENDPOINT["base"])
    
    def get_types(self):
        parliament_type_map = {
            "committee":"parliament-committee-types",
            "committee_business":"parliament-committee_business-types",
            "bill":"parliament-bill-types",
            "bill_publication":"parliament-bill_publication-types",
            "bill_stage":"parliamet-bill_stage-types"
        }
        types = []
        for parliament_endpoint, _ in parliament_type_map.items():
            if parliament_endpoint.startswith("committee"):
                parliament_types = self.committee_parley.__fetch__(self.COMMITTEE_ENDPOINT[parliament_endpoint])
                type_name = self.COMMITTEE_ENDPOINT[parliament_endpoint]
            else:
                parliament_types = self.bill_parley.__exhaustive_fetch__(self.BILL_ENDPOINT[parliament_endpoint])["items"]
                type_name = self.BILL_ENDPOINT[parliament_endpoint]
            for t in parliament_types:
                t.update({"parliamentType":type_name})
            types.extend(parliament_types)
        return types