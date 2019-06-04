# usatriathlon

## Dependencies

- Python3
- [Redis](https://redis.io/download)

The scraper uses Redis for caching. You will need to install Redis and update the scraper.py
to use the password you configure for Redis. It is currently set to 'foobared' as a placeholder.

## Setup

    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt

## Usage

    python scraper.py

The scraper will create a directory named results, under which the results will be stored broken
down by year/event/race.