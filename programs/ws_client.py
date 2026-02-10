"""
Client WebSocket pour la communication avec le serveur
"""

import asyncio
import json
import logging
import time
from typing import Callable, Optional
import websockets
from websockets.client import WebSocketClientProtocol

from config import Config

logger = logging.getLogger(__name__)


class WSClient:
    """Client WebSocket avec reconnexion automatique"""

    def __init__(self, url: str, password: str):
        self.url = url
        self.password = password
        self.ws: Optional[WebSocketClientProtocol] = None
        self.token: Optional[str] = None
        self.reconnect_attempts = 0
        self.is_running = False
        self.is_authenticated = False

        # Callbacks
        self.on_authenticated: Optional[Callable] = None
        self.on_message: Optional[Callable[[dict], None]] = None
        self.on_disconnected: Optional[Callable] = None
        self.on_error: Optional[Callable[[Exception], None]] = None

        # Stats d'envoi
        self._total_bytes_sent = 0
        self._total_messages_sent = 0

    async def connect(self):
        """Connecte au serveur WebSocket"""
        self.is_running = True

        while self.is_running and self.reconnect_attempts < Config.MAX_RECONNECT_ATTEMPTS:
            try:
                logger.info(f"Connexion à {self.url}...")

                async with websockets.connect(
                    self.url,
                    ping_interval=30,
                    ping_timeout=10,
                    close_timeout=10
                ) as ws:
                    self.ws = ws
                    self.reconnect_attempts = 0
                    logger.info("Connecté au serveur")

                    await self.authenticate()
                    await self.receive_loop()

            except websockets.exceptions.ConnectionClosed as e:
                logger.warning(f"Connexion fermée: {e}")
                await self.handle_disconnect()

            except Exception as e:
                logger.error(f"Erreur de connexion: {e}", exc_info=True)
                if self.on_error:
                    self.on_error(e)
                await self.handle_disconnect()

        if self.reconnect_attempts >= Config.MAX_RECONNECT_ATTEMPTS:
            logger.error("Nombre maximum de tentatives de reconnexion atteint")

    async def authenticate(self):
        """S'authentifie auprès du serveur"""
        logger.info("Authentification...")

        auth_message = {
            'type': 'AUTH',
            'password': self.password,
            'timestamp': int(time.time() * 1000)
        }

        await self.send(auth_message)

        try:
            response = await asyncio.wait_for(
                self.ws.recv(),
                timeout=30.0
            )

            message = json.loads(response)

            if message.get('type') == 'AUTH_SUCCESS':
                self.token = message.get('token')
                self.is_authenticated = True
                logger.info(f"Authentification réussie (token: {self.token[:8]}...)")

                if self.on_authenticated:
                    await self.on_authenticated(message)

            elif message.get('type') == 'AUTH_FAILED':
                reason = message.get('reason', 'Raison inconnue')
                logger.error(f"Authentification échouée: {reason}")
                self.is_running = False

        except asyncio.TimeoutError:
            logger.error("Timeout lors de l'authentification")
            self.is_running = False

    async def receive_loop(self):
        """Boucle de réception des messages"""
        while self.is_running and self.ws:
            try:
                message_str = await self.ws.recv()
                message = json.loads(message_str)
                await self.handle_message(message)

            except websockets.exceptions.ConnectionClosed:
                logger.warning("Connexion fermée pendant la réception")
                break

            except json.JSONDecodeError as e:
                logger.error(f"Erreur décodage JSON: {e}")

            except Exception as e:
                logger.error(f"Erreur dans receive_loop: {e}", exc_info=True)

    async def handle_message(self, message: dict):
        """Gère un message reçu"""
        msg_type = message.get('type')

        if msg_type == 'BLEND_FILE_URL':
            logger.info("URL .blend reçue")
        elif msg_type == 'BLEND_FILE':
            logger.info("Fichier .blend reçu (base64)")
        elif msg_type == 'S3_CREDENTIALS':
            logger.info("Credentials S3 reçues")
        elif msg_type == 'CACHE_DATA':
            logger.info("Données de cache reçues")
        elif msg_type == 'CACHE_DATA_URL':
            logger.info("URL cache reçue")
        elif msg_type == 'TERMINATE':
            reason = message.get('reason', 'Non spécifié')
            logger.warning(f"Demande de terminaison: {reason}")
            self.is_running = False
        elif msg_type == 'PONG':
            pass
        else:
            logger.debug(f"Message reçu: {msg_type}")

        if self.on_message:
            await self.on_message(message)

    async def send(self, message: dict):
        """Envoie un message au serveur"""
        if not self.ws:
            logger.warning("Impossible d'envoyer, pas de connexion")
            return False

        try:
            serialized = json.dumps(message)
            msg_size = len(serialized.encode('utf-8'))

            await self.ws.send(serialized)

            self._total_bytes_sent += msg_size
            self._total_messages_sent += 1
            return True
        except Exception as e:
            logger.error(f"Erreur envoi message: {e}")
            return False

    async def send_heartbeat(self):
        """Envoie un heartbeat"""
        return await self.send({'type': 'ALIVE'})

    async def send_progress(
        self,
        upload_percent: int,
        disk_bytes: int,
        disk_files: int,
        uploaded_bytes: int,
        uploaded_files: int,
        errors: int,
        rate_bytes_per_sec: float,
    ):
        """Envoie une mise à jour de progression (léger, pas de données binaires)."""
        return await self.send({
            'type': 'PROGRESS_UPDATE',
            'uploadPercent': upload_percent,
            'diskBytes': disk_bytes,
            'diskFiles': disk_files,
            'uploadedBytes': uploaded_bytes,
            'uploadedFiles': uploaded_files,
            'errors': errors,
            'rateBytesPerSec': int(rate_bytes_per_sec),
        })

    async def send_cache_complete(self):
        """Signale que le cache est complet"""
        return await self.send({'type': 'CACHE_COMPLETE'})

    async def send_ready_to_terminate(self):
        """Signale que la VM est prête à terminer"""
        return await self.send({'type': 'READY_TO_TERMINATE'})

    async def handle_disconnect(self):
        """Gère une déconnexion"""
        self.is_authenticated = False
        self.token = None

        if self.on_disconnected:
            self.on_disconnected()

        if self.is_running:
            self.reconnect_attempts += 1
            delay = Config.RECONNECT_DELAY * self.reconnect_attempts
            logger.info(
                f"Reconnexion dans {delay}s "
                f"(tentative {self.reconnect_attempts}/{Config.MAX_RECONNECT_ATTEMPTS})"
            )
            await asyncio.sleep(delay)

    def disconnect(self):
        """Déconnecte du serveur"""
        logger.info(
            f"Déconnexion... "
            f"(total envoyé: {self._total_messages_sent} msgs, "
            f"{self._total_bytes_sent} bytes)"
        )
        self.is_running = False
        if self.ws:
            asyncio.create_task(self.ws.close())

    def is_connected(self) -> bool:
        """Vérifie si connecté et authentifié"""
        return self.ws is not None and self.is_authenticated