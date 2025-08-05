from Services.BaseFetcher import BaseFetcher
from collections import defaultdict


class TeetimesFetcher(BaseFetcher):
    @property
    def url(self) -> str:
        return "https://www.masters.com/en_US/scores/feeds/2025/pairings.json"

    @property
    def topic_template(self) -> str:
        return "masters.TeeTimes*2025.{player_id}"

    def fetch(self) -> list[dict]:
        response = self._safe_request()
        data = response.json()

        players_data = defaultdict(lambda: {"teetimes": {}})

        for round_key in data:
            if not round_key.startswith("round"):
                continue

            round_number = round_key.replace("round", "")
            groups = data[round_key]["groups"]

            for group in groups:
                teetime = group.get("teetime")
                group_id = group.get("number")
                starting_hole = group.get("tee")
                players = group.get("players", [])

                for player in players:
                    player_id = player.get("id")
                    order = player.get("order")

                    players_data[player_id]["player_id"] = player_id
                    players_data[player_id]["teetimes"][round_number] = {
                        "time": teetime,
                        "group_id": group_id,
                        "starting_hole": starting_hole,
                        "order_of_play_within_group": order
                    }

        return list(players_data.values())

    def transform(self, player_teetimes: dict) -> dict:
        return player_teetimes