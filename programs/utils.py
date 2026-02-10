"""
Fonctions utilitaires
"""

import base64
import hashlib
import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

def setup_logging(level=logging.INFO):
    """Configure le logging"""
    logging.basicConfig(
        level=level,
        format='[%(asctime)s] %(levelname)s - %(name)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

def encode_file_to_base64(file_path: Path) -> str:
    """Encode un fichier en base64"""
    with open(file_path, 'rb') as f:
        return base64.b64encode(f.read()).decode('utf-8')

def decode_base64_to_file(data: str, file_path: Path) -> None:
    """Décode du base64 et sauvegarde dans un fichier"""
    with open(file_path, 'wb') as f:
        f.write(base64.b64decode(data))

def calculate_file_hash(file_path: Path) -> str:
    """Calcule le hash SHA256 d'un fichier"""
    sha256 = hashlib.sha256()
    
    with open(file_path, 'rb') as f:
        while True:
            data = f.read(65536)  # 64KB chunks
            if not data:
                break
            sha256.update(data)
    
    return sha256.hexdigest()

def get_cache_files(cache_dir: Path) -> list[Path]:
    """Récupère tous les fichiers de cache"""
    if not cache_dir.exists():
        return []
    
    # Cherche les fichiers de cache Blender (.bphys, .vdb, etc.)
    extensions = ['.bphys', '.vdb', '.png', '.exr', '.abc']
    files = []
    
    for ext in extensions:
        files.extend(cache_dir.rglob(f'*{ext}'))
    
    return sorted(files)

def chunk_file(file_path: Path, chunk_size: int = 1024 * 1024):
    """Générateur qui découpe un fichier en chunks"""
    with open(file_path, 'rb') as f:
        chunk_id = 0
        while True:
            data = f.read(chunk_size)
            if not data:
                break
            yield chunk_id, data
            chunk_id += 1

def format_bytes(bytes_count: int) -> str:
    """Formate une taille en bytes de façon lisible"""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_count < 1024.0:
            return f"{bytes_count:.2f} {unit}"
        bytes_count /= 1024.0
    return f"{bytes_count:.2f} PB"

def get_blender_cache_paths() -> list[Path]:
    """Retourne les chemins possibles pour le cache Blender"""
    from config import Config
    
    paths = [
        Config.CACHE_DIR,  # Cache local configuré
        Config.WORK_DIR / '.blend_cache',  # Cache projet
        Path.home() / '.cache' / 'blender',  # Cache utilisateur Linux
        Path('/tmp/blender_cache'),  # Cache temporaire
    ]
    
    return [p for p in paths if p.exists()]
