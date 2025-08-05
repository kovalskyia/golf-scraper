from services.BaseFetcher import BaseFetcher


class EntrylistFetcher(BaseFetcher):
    @property
    def url(self) -> str:
        return "https://www.masters.com/en_US/cms/feeds/players/2025/players.json"

    @property
    def topic_template(self) -> str:
        return "masters.Entrylist*2025.{player_id}"

    def transform(self, player: dict) -> dict:
        return {
            "player_id": player.get("id"),
            "name": player.get("name"),
            "country_code": player.get("countryCode"),
            "is_amateur": player.get("amateur", False)
        }