import os


class Config:
    def __init__(self):
        # RabbitMQ Configuration
        self.rabbitmq_host = os.getenv("RABBITMQ_HOST", "localhost")
        self.rabbitmq_port = int(os.getenv("RABBITMQ_PORT", "5672"))
        self.rabbitmq_user = os.getenv("RABBITMQ_USER", "admin")
        self.rabbitmq_password = os.getenv("RABBITMQ_PASSWORD", "admin123")
        self.rabbitmq_vhost = os.getenv("RABBITMQ_VHOST", "/")

        # Application Configuration
        self.log_level = os.getenv("LOG_LEVEL", "INFO")
        self.health_port = int(os.getenv("HEALTH_PORT", "8000"))

        # Scraping Configuration
        self.scrape_interval = int(os.getenv("SCRAPE_INTERVAL", "300"))  # 5 minutes
        self.request_timeout = int(os.getenv("REQUEST_TIMEOUT", "60"))
        self.max_retries = int(os.getenv("MAX_RETRIES", "3"))

        # Masters Tournament Configuration
        self.tournament_year = os.getenv("TOURNAMENT_YEAR", "2025")
        self.base_url = "https://www.masters.com"

        # API Endpoints
        self.endpoints = {
            "entrylist": f"{self.base_url}/en_US/cms/feeds/players/{self.tournament_year}/players.json",
            "teetimes": f"{self.base_url}/en_US/scores/feeds/{self.tournament_year}/pairings.json",
            "leaderboard": f"{self.base_url}/en_US/scores/feeds/{self.tournament_year}/scores.json",
            "shots": f"{self.base_url}/en_US/scores/feeds/{self.tournament_year}/track/{{player_id}}.json",
        }

        # RabbitMQ Topics
        self.topics = {
            "entrylist": f"masters.Entrylist*{self.tournament_year}.{{player_id}}",
            "teetimes": f"masters.TeeTimes*{self.tournament_year}.{{player_id}}",
            "leaderboard": f"masters.Leaderboard*{self.tournament_year}",
            "shots": f"masters.Shots*{self.tournament_year}.{{player_id}}",
        }
