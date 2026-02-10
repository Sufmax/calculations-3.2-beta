"""
Configuration pour le script VM
"""

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

class Config:
    """Configuration globale"""

    # WebSocket
    WS_URL = os.getenv('WS_URL', 'wss://your-worker.pages.dev/ws/vm')
    VM_PASSWORD = os.getenv('VM_PASSWORD')

    # Chemins
    BASE_DIR = Path(__file__).parent
    WORK_DIR = BASE_DIR / 'work'
    BLEND_FILE = WORK_DIR / 'current.blend'
    CACHE_DIR = WORK_DIR / 'cache'

    # Blender
    BLENDER_EXECUTABLE = os.getenv('BLENDER_EXECUTABLE', 'blender')
    BLENDER_SCRIPT = BASE_DIR / 'bake_all.py'

    # Threading pour le bake
    # Nombre de threads pour Mantaflow OpenMP et Blender render.threads
    # Défaut : cpu_count - 2 (réserve pour OS + cache_streamer)
    BAKE_THREADS = int(os.getenv('BAKE_THREADS', str(max(1, (os.cpu_count() or 1) - 2))))

    # Timing
    HEARTBEAT_INTERVAL = int(os.getenv('HEARTBEAT_INTERVAL', '3'))
    CACHE_CHECK_INTERVAL = float(os.getenv('CACHE_CHECK_INTERVAL', '2.0'))
    CHUNK_SIZE = int(os.getenv('CHUNK_SIZE', str(32 * 1024)))

    # Limites
    MAX_RECONNECT_ATTEMPTS = int(os.getenv('MAX_RECONNECT_ATTEMPTS', '10'))
    RECONNECT_DELAY = int(os.getenv('RECONNECT_DELAY', '5'))

    @classmethod
    def ensure_dirs(cls):
        cls.WORK_DIR.mkdir(parents=True, exist_ok=True)
        cls.CACHE_DIR.mkdir(parents=True, exist_ok=True)

    @classmethod
    def validate(cls):
        if not cls.VM_PASSWORD:
            raise ValueError(
                "VM_PASSWORD non défini. "
                "Définissez-le dans .env ou comme variable d'environnement"
            )
        cls.ensure_dirs()
        return True