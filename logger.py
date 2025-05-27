import logging
from logging import StreamHandler, Formatter
from pathlib import Path
from datetime import datetime
import os
from logging.handlers import RotatingFileHandler

class CustomRotatingFileHandler(RotatingFileHandler):
    def doRollover(self):
        if self.stream:
            self.stream.close()
            self.stream = None
        # Shift old logs: .04.log <- .03.log <- ... <- .01.log
        for i in range(self.backupCount - 1, 0, -1):
            sfn = f"{self.baseFilename[:self.baseFilename.rfind('.log')]}_" + f"{i:#04x}"[2:] + ".log"
            dfn = f"{self.baseFilename[:self.baseFilename.rfind('.log')]}_" + f"{i+1:#04x}"[2:] + ".log"
            if os.path.exists(sfn):
                if os.path.exists(dfn):
                    os.remove(dfn)
                os.rename(sfn, dfn)
        # Rename current log to .1.log
        dfn = f"{self.baseFilename[:self.baseFilename.rfind('.log')]}_01.log"
        if os.path.exists(dfn):
            os.remove(dfn)
        if os.path.exists(self.baseFilename):
            os.rename(self.baseFilename, dfn)
        self.stream = self._open()

def init_logging(file_level=logging.INFO, console_level=logging.WARNING, log_dir="log", log_file_name="experiment-orchestrator", log_format="%(asctime)s %(levelname)s [%(process)d:%(threadName)s] %(funcName)s: %(message)s", max_bytes=10*1024*1024, backup_count=5):
    """
    Initializes logging: root logger, console and file handler with rotation.
    Logs are written to log/experiment-orchestrator_YYYYMMDD.log and archives experiment-orchestrator_YYYYMMDD.NN.log
    """
    log_formatter = Formatter(log_format)
    logger = logging.getLogger()
    logger.setLevel(file_level)

    # Console Handler (WARNING)
    console_handler = StreamHandler()
    console_handler.setFormatter(log_formatter)
    console_handler.setLevel(console_level)
    logger.addHandler(console_handler)

    # File Handler (INFO+), rotation by size, up to 5 archives
    log_dir = Path(__file__).parent / log_dir
    log_dir.mkdir(exist_ok=True)
    log_file_base = log_dir / f"{log_file_name}_{datetime.now().strftime('%Y-%m-%d')}.log"
    file_handler = CustomRotatingFileHandler(
        str(log_file_base),
        maxBytes=max_bytes,
        backupCount=backup_count
    )
    file_handler.setFormatter(log_formatter)
    file_handler.setLevel(file_level)
    logger.addHandler(file_handler)
    logging.info(f"Logging configured: Console={logging.getLevelName(console_level)}, File={logging.getLevelName(file_level)} ({str(log_file_base)})")
