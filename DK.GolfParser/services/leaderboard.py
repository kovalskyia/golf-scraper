import structlog

from typing import Dict, Any, List

from services.BaseFetcher import BaseFetcher
from services.config import Config

logger = structlog.get_logger(__name__)

class LeaderboardFetcher(BaseFetcher):
    def __init__(self):
        self.config = Config()
        super()

    @property
    def url(self) -> str:
        return self.config.endpoints['leaderboard']

    @property
    def topic_template(self) -> str:
        return self.config.topics['leaderboard']

    def _process_thru(self, thru_value: str) -> int:
        if thru_value == 'f':
            return 18
        try:
            return int(thru_value)
        except (ValueError, TypeError):
            return 0 

    def transform(self, raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        processed_players = []
            
        for player in raw_data.get('players', []):
            processed_player = {
                'player_id': str(player.get('id', '')),
                'current_position': int(player.get('pos', 0)),
                'round_score': int(player.get('today', 0)),
                'thru': self._process_thru(player.get('thru', '')),
                'total': int(player.get('topar', 0)),
                'round_strokes': {}
            }
            
            # Process round strokes
            for round_num in ['1', '2', '3', '4']:
                round_key = f'round{round_num}'
                if round_key in player:
                    processed_player['round_strokes'][round_num] = int(player[round_key].get('total', 0))
            
            processed_players.append(processed_player)
        
        logger.info("Processed leaderboard data", player_count=len(processed_players))
        return processed_players