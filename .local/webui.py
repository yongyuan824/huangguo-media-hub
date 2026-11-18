import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

import uvicorn

from HGXZ.cli import Archiver, load_config
from HGXZ.web.app import create_app, set_shared_state
from HGXZ.web.log_handler import LogBuffer

config = load_config(Path("config.json"))
log_dir = Path(config["log"]["directory"])
log_dir.mkdir(parents=True, exist_ok=True)
formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
log_buffer = LogBuffer()
log_file = TimedRotatingFileHandler(
    log_dir / "hgxz.log", when="midnight", backupCount=30, encoding="utf-8"
)
logger = logging.getLogger("HGXZ")
logger.setLevel(logging.INFO)
for handler in (log_buffer, log_file):
    handler.setFormatter(formatter)
    logger.addHandler(handler)
archiver = Archiver(config)
set_shared_state(archiver.state, log_buffer, [archiver])
try:
    uvicorn.run(create_app(), host="127.0.0.1", port=8099)
finally:
    archiver.close()
