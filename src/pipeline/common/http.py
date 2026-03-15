from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def build_session() -> Session:
    session = Session()
    retries = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(
        {"User-Agent": "PokemonBigDataBot/3.0 (Bachelor Project Medallion)"}
    )
    return session