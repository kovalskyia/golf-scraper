import structlog

from typing import Dict, Any, List
from services.metrics import metrics

logger = structlog.get_logger(__name__)


class DataProcessor:

    @staticmethod
    def process_entrylist(raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        try:
            processed_players = []

            for player in raw_data.get("players", []):
                player_id = str(player.get("id", "")).strip()

                # Skip players without a valid ID
                if not player_id:
                    logger.warning(
                        "Skipping player without valid ID",
                        player_name=player.get("name", "Unknown"),
                    )
                    continue

                processed_player = {
                    "player_id": player_id,
                    "name": player.get("name", ""),
                    "country_code": player.get("countryCode", ""),
                    "is_amateur": bool(player.get("amateur", False)),
                }
                processed_players.append(processed_player)

            logger.info("Processed entrylist data", player_count=len(processed_players))
            return processed_players

        except Exception as e:
            metrics.record_parse_error("entrylist", type(e).__name__)
            logger.error("Failed to process entrylist data", error=str(e))
            raise

    @staticmethod
    def process_teetimes(raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        try:
            processed_players = []

            for round_num in ["1", "2", "3", "4"]:
                round_key = f"round{round_num}"
                if round_key in raw_data:
                    for group in raw_data[round_key].get("groups", []):
                        for player in group.get("players", []):
                            player_id = str(player.get("id", "")).strip()

                            # Skip players without a valid ID
                            if not player_id:
                                logger.warning(
                                    "Skipping player without valid ID in tee times",
                                    player_name=player.get("name", "Unknown"),
                                )
                                continue

                            # Find existing player or create new one
                            existing_player = next(
                                (
                                    p
                                    for p in processed_players
                                    if p["player_id"] == player_id
                                ),
                                None,
                            )

                            if not existing_player:
                                existing_player = {
                                    "player_id": player_id,
                                    "teetimes": {},
                                }
                                processed_players.append(existing_player)

                            # Add tee time for this round
                            existing_player["teetimes"][round_num] = {
                                "time": group.get("teetime", ""),
                                "group_id": str(group.get("number", "")),
                                "starting_hole": int(group.get("tee", 1)),
                                "order_of_play_within_group": int(
                                    player.get("order", 1)
                                ),
                            }

            logger.info("Processed tee times data", player_count=len(processed_players))
            return processed_players

        except Exception as e:
            metrics.record_parse_error("teetimes", type(e).__name__)
            logger.error("Failed to process tee times data", error=str(e))
            raise

    @staticmethod
    def process_leaderboard(raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        try:
            processed_players = []

            for player in raw_data.get("players", []):
                player_id = str(player.get("id", "")).strip()

                # Skip players without a valid ID
                if not player_id:
                    logger.warning(
                        "Skipping player without valid ID in leaderboard",
                        player_name=player.get("name", "Unknown"),
                    )
                    continue

                processed_player = {
                    "player_id": player_id,
                    "current_position": DataProcessor._process_position(
                        player.get("pos", 0)
                    ),
                    "round_score": int(player.get("today", 0)),
                    "thru": DataProcessor._process_thru(player.get("thru", "")),
                    "total": int(player.get("topar", 0)),
                    "round_strokes": {},
                }

                # Process round strokes
                for round_num in ["1", "2", "3", "4"]:
                    round_key = f"round{round_num}"
                    if round_key in player:
                        processed_player["round_strokes"][round_num] = int(
                            player[round_key].get("total", 0)
                        )

                processed_players.append(processed_player)

            logger.info(
                "Processed leaderboard data", player_count=len(processed_players)
            )
            return processed_players

        except Exception as e:
            metrics.record_parse_error("leaderboard", type(e).__name__)
            logger.error("Failed to process leaderboard data", error=str(e))
            raise

    @staticmethod
    def process_shots(raw_data: Dict[str, Any], player_id: str) -> Dict[str, Any]:
        try:
            processed_shots = {"player_id": player_id}

            for round_data in raw_data.get("rounds", []):
                round_num = str(round_data.get("id", ""))
                processed_shots[round_num] = {}

                for hole_data in round_data.get("holes", []):
                    hole_num = str(hole_data.get("id", ""))
                    processed_shots[round_num][hole_num] = {}

                    shot_count = 0
                    for shot_data in hole_data.get("shots", []):
                        shot_num = str(shot_data.get("num", ""))

                        # Determine surface based on remaining distance and ongreen flag
                        remaining = float(shot_data.get("remaining", 0.0))
                        ongreen = shot_data.get("ongreen", False)
                        surface = DataProcessor._get_surface(remaining, ongreen)

                        processed_shots[round_num][hole_num][shot_num] = {
                            "date": shot_data.get("date", ""),
                            "time": shot_data.get("time", ""),
                            "surface": surface,
                            "distance": float(shot_data.get("length", 0.0)),
                            "distance_to_hole": float(shot_data.get("remaining", 0.0)),
                        }
                        shot_count += 1

                    # Record shots per hole metric
                    metrics.record_shots_per_hole(
                        int(round_num), int(hole_num), shot_count
                    )

            logger.info("Processed shots data", player_id=player_id)
            return processed_shots

        except Exception as e:
            metrics.record_parse_error("shots", type(e).__name__)
            logger.error(
                "Failed to process shots data", player_id=player_id, error=str(e)
            )
            raise

    @staticmethod
    def _process_position(position_value: Any) -> int:
        if isinstance(position_value, str):
            # Handle tied positions like "T2" by stripping "T" and converting to int
            if position_value.startswith("T"):
                try:
                    return int(position_value[1:])
                except ValueError:
                    logger.warning(f"Invalid tied position format: {position_value}")
                    return 0
            else:
                try:
                    return int(position_value)
                except ValueError:
                    logger.warning(f"Invalid position format: {position_value}")
                    return 0
        else:
            try:
                return int(position_value)
            except (ValueError, TypeError):
                return 0

    @staticmethod
    def _get_surface(remaining_distance: float, ongreen: bool) -> str:
        if remaining_distance == 0.0:
            return "hole"
        elif ongreen:
            return "green"
        else:
            return "unknown"

    @staticmethod
    def _process_thru(thru_value: str) -> int:
        if thru_value == "f":
            return 18
        try:
            return int(thru_value)
        except (ValueError, TypeError):
            return 0
