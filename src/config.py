# In bytes
CLIENT_RPC_TIMEOUT = 10
MASTER_RPC_TIMEOUT = 30
CHUNK_RPC_TIMEOUT = 100
HEARTBEAT_INTERVAL = 2
HEARTBEAT_TIMEOUT = 1
# In terms of number of heartbeats
FILE_CLEANUP_INTERVAL = 5
FILE_CLEANUP_THRESHOLD = 120
CHUNK_CLEANUP_INTERVAL = 10
CHUNK_CLEANUP_THRESHOLD = 120
# In terms of chunk cleanup intervals
CHUNK_AUTO_QUERY = 50
# In terms of number of super heartbeats
REBALANCE_INTERVAL = 50
REBALANCE_CHUNK_THRESHOLD = 20
REBALANCE_DIFF_PERCENTAGE = 20
REBALANCE_MAX_CHUNKS_MOVED = 10
CLIENT_RETRY_LIMIT = 5
CLIENT_RETRY_INTERVAL = 5
CHUNK_SIZE = 1024
PACKET_SIZE = 128
REPLICATION_FACTOR = 3
CHUNK_PORTS = ["50052", "50053", "50054", "50055"]
CHUNK_IPS = ["localhost", "localhost", "localhost", "localhost"]
CHUNK_ROOT_DIRS = ["/tmp/chunk1", "/tmp/chunk2", "/tmp/chunk3", "/tmp/chunk4"]
NUM_CHUNK_SERVERS = len(CHUNK_IPS)
CHUNK_LOCS = [CHUNK_IPS[i] + ":" + CHUNK_PORTS[i] for i in range(NUM_CHUNK_SERVERS)]
MASTER_PORT = "50051"
MASTER_IP = "localhost"
MASTER_LOC = MASTER_IP + ":" + MASTER_PORT
MASTER_LOG = '/tmp/master.log'
LOGGER_CONFIG = '''
{{
    "version": 1,
    "disable_existing_loggers": false,
    "formatters": {{
        "simple": {{
            "format": "%(message)s"
        }},
        "debug": {{
            "format": "%(asctime)s %(levelname)s:%(message)s",
            "datefmt": "%Y-%m-%d %H:%M"
        }}
    }},
    "filters": {{
        "info_and_below": {{
            "()" : "utils.filter_maker",
            "level": "INFO"
        }}
    }},
    "handlers": {{
        "stdout": {{
            "class": "logging.StreamHandler",
            "formatter": "debug",
            "stream": "ext://sys.stdout"
        }},
        "file": {{
            "class": "logging.FileHandler",
            "formatter": "simple",
            "filename": "{0}",
            "mode": "a",
            "level": "INFO",
            "filters": ["info_and_below"]
        }}
    }},
    "root": {{
        "level": "DEBUG",
        "handlers": [
            "stdout",
            "file"
        ]
    }}
}}
'''.format(MASTER_LOG)
