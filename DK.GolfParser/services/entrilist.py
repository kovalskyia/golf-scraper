from services.BaseFetcher import BaseFetcher
from services.config import Config

class EntrylistFetcher(BaseFetcher):

    def __init__(self):
        self.config = Config()
        super()

    @property
    def url(self) -> str:
        return self.config.endpoints['entrylist']

    @property
    def topic_template(self) -> str:
        return self.config.topics['entrylist']

    def transform(self, player: dict) -> dict:
        return {
            "player_id": player.get("id"),
            "name": player.get("name"),
            "country_code": player.get("countryCode"),
            "is_amateur": player.get("amateur", False)
        }