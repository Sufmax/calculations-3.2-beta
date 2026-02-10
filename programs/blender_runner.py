"""
Gestionnaire du processus Blender
"""

import asyncio
import logging
import os
import subprocess
import signal
from pathlib import Path
from typing import Optional

from config import Config

logger = logging.getLogger(__name__)


class BlenderRunner:
    """Gère l'exécution de Blender"""

    def __init__(self, blend_file: Path, cache_dir: Path):
        self.blend_file = blend_file
        self.cache_dir = cache_dir
        self.process: Optional[subprocess.Popen] = None
        self.is_running = False

    async def run(self):
        """Lance Blender avec le script de baking"""
        if not self.blend_file.exists():
            raise FileNotFoundError(f"Fichier .blend introuvable: {self.blend_file}")

        if not Config.BLENDER_SCRIPT.exists():
            raise FileNotFoundError(f"Script Blender introuvable: {Config.BLENDER_SCRIPT}")

        logger.info(f"Lancement de Blender: {self.blend_file}")

        # Commande Blender
        cmd = [
            Config.BLENDER_EXECUTABLE,
            '--background',
            str(self.blend_file),
            '--python', str(Config.BLENDER_SCRIPT),
            '--',
            '--cache-dir', str(self.cache_dir),
        ]

        logger.info(f"Commande: {' '.join(cmd)}")

        # Environnement avec OMP_NUM_THREADS pour Mantaflow multi-thread
        env = os.environ.copy()
        env["OMP_NUM_THREADS"] = str(Config.BAKE_THREADS)
        # Pas de binding CPU pour laisser le scheduler optimiser
        env.pop("OMP_PROC_BIND", None)
        logger.info(f"OMP_NUM_THREADS={Config.BAKE_THREADS}")

        try:
            self.process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
                universal_newlines=True,
                env=env,
            )

            self.is_running = True
            logger.info(f"Blender démarré (PID: {self.process.pid})")

            await self.stream_output()

            return_code = self.process.wait()

            self.is_running = False
            logger.info(f"Blender terminé (code: {return_code})")

            return return_code

        except FileNotFoundError:
            logger.error(
                f"Blender introuvable. Vérifiez que '{Config.BLENDER_EXECUTABLE}' "
                "est dans le PATH ou configurez BLENDER_EXECUTABLE"
            )
            raise

        except Exception as e:
            logger.error(f"Erreur lancement Blender: {e}", exc_info=True)
            self.is_running = False
            raise

    async def stream_output(self):
        """Stream les sorties stdout et stderr de Blender"""
        if not self.process:
            return

        async def read_stream(stream, prefix):
            loop = asyncio.get_event_loop()
            while self.is_running:
                try:
                    line = await loop.run_in_executor(None, stream.readline)
                    if not line:
                        break
                    line = line.strip()
                    if line:
                        logger.info(f"[Blender {prefix}] {line}")
                except Exception as e:
                    logger.error(f"Erreur lecture {prefix}: {e}")
                    break

        await asyncio.gather(
            read_stream(self.process.stdout, 'stdout'),
            read_stream(self.process.stderr, 'stderr'),
            return_exceptions=True
        )

    def terminate(self, graceful: bool = True):
        """Termine le processus Blender"""
        if not self.process or not self.is_running:
            return

        logger.info("Arrêt de Blender...")

        if graceful:
            try:
                self.process.send_signal(signal.SIGTERM)
                try:
                    self.process.wait(timeout=30)
                    logger.info("Blender arrêté proprement")
                except subprocess.TimeoutExpired:
                    logger.warning("Timeout arrêt gracieux, force kill")
                    self.process.kill()
            except Exception as e:
                logger.error(f"Erreur arrêt gracieux: {e}")
                self.process.kill()
        else:
            self.process.kill()
            logger.info("Blender tué (SIGKILL)")

        self.is_running = False

    def is_alive(self) -> bool:
        if not self.process:
            return False
        return self.process.poll() is None