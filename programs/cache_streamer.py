"""
Streamer de cache Blender — upload direct vers Storj.

Au lieu de transférer les chunks via WebSocket (lent, base64 +33%),
les fichiers de cache sont uploadés directement vers Storj S3 par la VM.
Seuls les messages de progression transitent par le WebSocket.
"""

import asyncio
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Dict, Optional, Set

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileSystemEvent

from config import Config
from s3_uploader import S3Uploader
from utils import format_bytes

logger = logging.getLogger(__name__)

CACHE_EXTENSIONS = {
    '.bphys', '.vdb', '.uni', '.gz',
    '.png', '.exr', '.abc', '.obj', '.ply',
}

# Intervalle entre les rapports de progression (secondes)
PROGRESS_INTERVAL = 5.0

# Nombre d'uploads parallèles
UPLOAD_WORKERS = 3


class CacheFileHandler(FileSystemEventHandler):
    """Handler watchdog — détecte les fichiers de cache."""

    def __init__(self, streamer: 'CacheStreamer'):
        self.streamer = streamer
        super().__init__()

    def _should_process(self, src_path: str) -> bool:
        return Path(src_path).suffix.lower() in CACHE_EXTENSIONS

    def on_created(self, event: FileSystemEvent):
        if not event.is_directory and self._should_process(event.src_path):
            self.streamer.schedule_file(Path(event.src_path))

    def on_modified(self, event: FileSystemEvent):
        if not event.is_directory and self._should_process(event.src_path):
            self.streamer.schedule_file(Path(event.src_path))


class CacheStreamer:
    """Upload direct des fichiers de cache vers Storj S3.

    Architecture :
    - Le watchdog surveille le répertoire cache (thread séparé)
    - Les fichiers détectés sont mis en queue asyncio
    - Un pool de threads effectue les uploads S3 en parallèle
    - Un task asyncio envoie la progression via WebSocket toutes les 5s
    """

    def __init__(
        self,
        cache_dir: Path,
        ws_client,
        s3_credentials: Dict[str, str],
    ):
        self.cache_dir = cache_dir
        self.ws_client = ws_client

        # Initialiser l'uploader S3 direct
        self.uploader = S3Uploader(
            endpoint=s3_credentials['endpoint'],
            bucket=s3_credentials['bucket'],
            region=s3_credentials['region'],
            access_key_id=s3_credentials['accessKeyId'],
            secret_access_key=s3_credentials['secretAccessKey'],
            cache_prefix=s3_credentials.get('cachePrefix', 'cache/'),
        )

        self.queue: asyncio.Queue = asyncio.Queue()
        self.uploaded_files: Set[str] = set()
        self.pending_files: Set[str] = set()
        self.failed_files: Set[str] = set()
        self.is_running = False
        self.observer: Optional[Observer] = None
        self.upload_task: Optional[asyncio.Task] = None
        self.progress_task: Optional[asyncio.Task] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._executor = ThreadPoolExecutor(max_workers=UPLOAD_WORKERS)

        # Stats
        self.start_time = time.time()

    def start(self):
        """Démarre le streamer."""
        logger.info(f"Démarrage du streamer (upload direct Storj): {self.cache_dir}")
        logger.info(
            f"  Endpoint : {self.uploader.endpoint}"
            f"  Bucket   : {self.uploader.bucket}"
            f"  Prefix   : {self.uploader.cache_prefix}"
            f"  Workers  : {UPLOAD_WORKERS}"
        )
        self.is_running = True
        self._loop = asyncio.get_event_loop()

        self._start_watching()
        self.upload_task = asyncio.create_task(self._upload_loop())
        self.progress_task = asyncio.create_task(self._progress_loop())
        self._scan_existing_files()

    def stop(self):
        """Arrête le streamer proprement."""
        logger.info("Arrêt du streamer de cache")
        self.is_running = False

        if self.observer:
            self.observer.stop()
            self.observer.join(timeout=5)

        if self.upload_task:
            self.upload_task.cancel()
        if self.progress_task:
            self.progress_task.cancel()

        self._executor.shutdown(wait=False)

    def _start_watching(self):
        self.observer = Observer()
        handler = CacheFileHandler(self)
        self.observer.schedule(handler, str(self.cache_dir), recursive=True)
        self.observer.start()
        logger.info("Surveillance du cache activée")

    def _scan_existing_files(self):
        if not self.cache_dir.exists():
            logger.warning(f"Répertoire cache inexistant: {self.cache_dir}")
            return

        count = 0
        for ext in CACHE_EXTENSIONS:
            for fp in self.cache_dir.rglob(f'*{ext}'):
                if fp.is_file():
                    self._queue_file(fp)
                    count += 1
        logger.info(f"{count} fichiers de cache existants trouvés")

    def schedule_file(self, file_path: Path):
        """Appelé depuis le thread watchdog — thread-safe."""
        if self._loop is None or not self.is_running:
            return
        try:
            self._loop.call_soon_threadsafe(self._queue_file, file_path)
        except RuntimeError:
            pass

    def _queue_file(self, file_path: Path):
        """Ajoute un fichier à la queue (sur le loop asyncio)."""
        key = str(file_path)
        if key in self.uploaded_files or key in self.pending_files:
            return
        self.pending_files.add(key)
        self.queue.put_nowait(file_path)

    async def _upload_loop(self):
        """Boucle principale : prend les fichiers de la queue et les uploade."""
        logger.info("Boucle d'upload démarrée")
        try:
            while self.is_running:
                try:
                    file_path = await asyncio.wait_for(
                        self.queue.get(), timeout=1.0
                    )
                    await self._upload_file(file_path)
                except asyncio.TimeoutError:
                    continue
                except Exception as e:
                    logger.error(f"Erreur dans upload_loop: {e}", exc_info=True)
                    await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            logger.info("Upload loop annulée")
        logger.info("Boucle d'upload terminée")

    async def _upload_file(self, file_path: Path):
        """Upload un fichier vers Storj via le ThreadPoolExecutor."""
        file_key = str(file_path)

        if file_key in self.uploaded_files:
            self.pending_files.discard(file_key)
            return

        if not file_path.exists():
            logger.warning(f"Fichier disparu: {file_path}")
            self.pending_files.discard(file_key)
            return

        # Attendre stabilité
        await self._wait_file_stable(file_path)

        try:
            file_size = file_path.stat().st_size
        except OSError:
            self.pending_files.discard(file_key)
            return

        if file_size == 0:
            self.uploaded_files.add(file_key)
            self.pending_files.discard(file_key)
            return

        s3_key = self.uploader.build_s3_key(self.cache_dir, file_path)

        logger.info(
            f"Upload: {file_path.name} ({format_bytes(file_size)}) → {s3_key}"
        )

        # Upload dans le ThreadPoolExecutor (non-bloquant pour asyncio)
        loop = asyncio.get_event_loop()
        success = await loop.run_in_executor(
            self._executor,
            self.uploader.upload_file,
            file_path,
            s3_key,
        )

        if success:
            self.uploaded_files.add(file_key)
            self.pending_files.discard(file_key)
            logger.info(f"✓ {file_path.name} uploadé ({format_bytes(file_size)})")
        else:
            self.failed_files.add(file_key)
            self.pending_files.discard(file_key)
            logger.error(f"✗ {file_path.name} échoué")

    async def _wait_file_stable(self, file_path: Path, max_wait: float = 5.0):
        """Attend que le fichier soit stable (taille constante)."""
        last_size = -1
        waited = 0.0
        interval = 0.5
        while waited < max_wait:
            try:
                size = file_path.stat().st_size
                if size == last_size and size > 0:
                    return
                last_size = size
            except OSError:
                pass
            await asyncio.sleep(interval)
            waited += interval

    async def _progress_loop(self):
        """Envoie la progression toutes les PROGRESS_INTERVAL secondes."""
        logger.info(f"Boucle de progression démarrée (intervalle: {PROGRESS_INTERVAL}s)")
        try:
            while self.is_running:
                await asyncio.sleep(PROGRESS_INTERVAL)
                await self._send_progress()
        except asyncio.CancelledError:
            pass

    async def _send_progress(self):
        """Calcule et envoie la progression au serveur."""
        if not self.ws_client.is_connected():
            return

        # Scanner le disque pour les fichiers de cache
        disk_bytes = 0
        disk_files = 0
        if self.cache_dir.exists():
            for ext in CACHE_EXTENSIONS:
                for fp in self.cache_dir.rglob(f'*{ext}'):
                    try:
                        if fp.is_file():
                            disk_bytes += fp.stat().st_size
                            disk_files += 1
                    except OSError:
                        pass

        stats = self.uploader.get_stats()
        uploaded_bytes = stats['total_bytes_uploaded']
        uploaded_files = stats['total_files_uploaded']

        # Pourcentage basé sur les bytes
        if disk_bytes > 0:
            percent = min(100, int((uploaded_bytes / disk_bytes) * 100))
        else:
            percent = 0

        elapsed = time.time() - self.start_time
        rate = uploaded_bytes / elapsed if elapsed > 0 else 0

        await self.ws_client.send_progress(
            upload_percent=percent,
            disk_bytes=disk_bytes,
            disk_files=disk_files,
            uploaded_bytes=uploaded_bytes,
            uploaded_files=uploaded_files,
            errors=stats['total_errors'],
            rate_bytes_per_sec=rate,
        )

    async def finalize(self):
        """Finalise : vide la queue, attend les uploads, envoie CACHE_COMPLETE."""
        logger.info("Finalisation du streaming...")

        # Attendre que la queue soit vide
        timeout = 60.0
        waited = 0.0
        while not self.queue.empty() and waited < timeout:
            await asyncio.sleep(0.5)
            waited += 0.5

        # Attendre les uploads en cours (pending_files)
        waited = 0.0
        while self.pending_files and waited < timeout:
            await asyncio.sleep(0.5)
            waited += 0.5

        # Envoyer la progression finale
        await self._send_progress()

        # Signaler la fin
        await self.ws_client.send_cache_complete()

        stats = self.uploader.get_stats()
        elapsed = time.time() - self.start_time
        rate = stats['total_bytes_uploaded'] / elapsed if elapsed > 0 else 0

        logger.info(
            f"Upload terminé: {stats['total_files_uploaded']} fichiers, "
            f"{format_bytes(stats['total_bytes_uploaded'])} en {elapsed:.1f}s "
            f"({format_bytes(rate)}/s), "
            f"{stats['total_errors']} erreurs"
        )

    def get_stats(self) -> dict:
        stats = self.uploader.get_stats()
        elapsed = time.time() - self.start_time
        return {
            **stats,
            'elapsed_seconds': elapsed,
            'bytes_per_second': stats['total_bytes_uploaded'] / elapsed if elapsed > 0 else 0,
            'files_on_disk': len(self.uploaded_files) + len(self.pending_files) + len(self.failed_files),
            'files_pending': len(self.pending_files),
            'files_failed': len(self.failed_files),
        }