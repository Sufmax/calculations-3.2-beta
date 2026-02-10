"""
Uploader S3 direct pour cache Blender → Storj.
Implémente AWS Signature V4 avec uniquement la bibliothèque standard Python.
Aucune dépendance externe (pas de boto3, pas de requests).
"""

import hashlib
import hmac
import logging
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional, Tuple

logger = logging.getLogger(__name__)

# Content-types pour les fichiers de cache Blender
CONTENT_TYPES: Dict[str, str] = {
    '.bphys': 'application/octet-stream',
    '.vdb': 'application/octet-stream',
    '.uni': 'application/octet-stream',
    '.gz': 'application/gzip',
    '.png': 'image/png',
    '.exr': 'application/octet-stream',
    '.abc': 'application/octet-stream',
    '.obj': 'text/plain',
    '.ply': 'application/octet-stream',
}


class S3Uploader:
    """Client S3 direct avec AWS Signature V4.

    Utilisé par la VM pour uploader les fichiers de cache directement
    vers Storj, sans passer par le Worker Cloudflare.
    """

    def __init__(
        self,
        endpoint: str,
        bucket: str,
        region: str,
        access_key_id: str,
        secret_access_key: str,
        cache_prefix: str = 'cache/',
    ):
        self.endpoint = endpoint.rstrip('/')
        self.bucket = bucket
        self.region = region
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self.cache_prefix = cache_prefix

        # Extraire le hostname pour le header Host dans la signature
        parsed = urllib.parse.urlparse(self.endpoint)
        self.hostname = parsed.hostname or ''

        # Stats
        self.total_bytes_uploaded = 0
        self.total_files_uploaded = 0
        self.total_errors = 0
        self.last_error: Optional[str] = None

    def upload_file(
        self,
        file_path: Path,
        s3_key: str,
        content_type: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
        max_retries: int = 3,
    ) -> bool:
        """Upload un fichier vers Storj S3.

        Args:
            file_path: Chemin local du fichier
            s3_key: Clé S3 complète (ex: cache/vm_007/fluids/data.vdb)
            content_type: Type MIME (auto-détecté si None)
            metadata: Métadonnées x-amz-meta-*
            max_retries: Nombre de tentatives

        Returns:
            True si l'upload a réussi
        """
        if not file_path.exists():
            logger.warning(f"Fichier introuvable: {file_path}")
            return False

        if content_type is None:
            content_type = CONTENT_TYPES.get(
                file_path.suffix.lower(), 'application/octet-stream'
            )

        data = file_path.read_bytes()
        file_size = len(data)

        for attempt in range(1, max_retries + 1):
            try:
                self._put_object(s3_key, data, content_type, metadata or {})
                self.total_bytes_uploaded += file_size
                self.total_files_uploaded += 1
                return True

            except Exception as e:
                self.last_error = str(e)
                if attempt < max_retries:
                    delay = 2 ** (attempt - 1)
                    logger.warning(
                        f"Upload échoué ({attempt}/{max_retries}): "
                        f"{file_path.name} → {e} — retry dans {delay}s"
                    )
                    time.sleep(delay)
                else:
                    self.total_errors += 1
                    logger.error(
                        f"Upload abandonné après {max_retries} tentatives: "
                        f"{file_path.name} → {e}"
                    )
                    return False

        return False

    def build_s3_key(self, cache_dir: Path, file_path: Path) -> str:
        """Construit la clé S3 à partir du chemin local relatif au cache_dir.

        Ex: cache_dir=/work/cache, file_path=/work/cache/fluids/data_0001.vdb
            → cache/vm_007/fluids/data_0001.vdb
        """
        relative = file_path.relative_to(cache_dir)
        # Normaliser les séparateurs Windows → /
        rel_posix = relative.as_posix()
        return f"{self.cache_prefix}{rel_posix}"

    def _put_object(
        self,
        key: str,
        data: bytes,
        content_type: str,
        metadata: Dict[str, str],
    ) -> None:
        """Effectue un PUT S3 signé avec AWS Signature V4."""
        now = datetime.now(timezone.utc)
        amz_date = now.strftime('%Y%m%dT%H%M%SZ')
        date_stamp = now.strftime('%Y%m%d')

        # Encoder la clé segment par segment
        encoded_key = '/'.join(
            urllib.parse.quote(seg, safe='') for seg in key.split('/')
        )

        # URL complète
        url = f"{self.endpoint}/{self.bucket}/{encoded_key}"

        # Hash du payload
        payload_hash = hashlib.sha256(data).hexdigest()

        # Headers à signer (lowercase, triés)
        headers_to_sign: Dict[str, str] = {
            'content-length': str(len(data)),
            'content-type': content_type,
            'host': self.hostname,
            'x-amz-content-sha256': payload_hash,
            'x-amz-date': amz_date,
        }

        # Ajouter les métadonnées
        for meta_key, meta_value in metadata.items():
            headers_to_sign[f'x-amz-meta-{meta_key.lower()}'] = meta_value

        # Canonical request
        sorted_headers = sorted(headers_to_sign.items())
        canonical_headers = ''.join(f'{k}:{v}\n' for k, v in sorted_headers)
        signed_headers_list = ';'.join(k for k, _ in sorted_headers)

        canonical_request = '\n'.join([
            'PUT',
            f'/{self.bucket}/{encoded_key}',
            '',  # query string vide
            canonical_headers,
            signed_headers_list,
            payload_hash,
        ])

        # String to sign
        credential_scope = f'{date_stamp}/{self.region}/s3/aws4_request'
        canonical_hash = hashlib.sha256(
            canonical_request.encode('utf-8')
        ).hexdigest()

        string_to_sign = '\n'.join([
            'AWS4-HMAC-SHA256',
            amz_date,
            credential_scope,
            canonical_hash,
        ])

        # Signing key
        signing_key = self._get_signing_key(date_stamp)

        # Signature
        signature = hmac.new(
            signing_key,
            string_to_sign.encode('utf-8'),
            hashlib.sha256,
        ).hexdigest()

        # Authorization header
        authorization = (
            f'AWS4-HMAC-SHA256 '
            f'Credential={self.access_key_id}/{credential_scope}, '
            f'SignedHeaders={signed_headers_list}, '
            f'Signature={signature}'
        )

        # Headers de la requête HTTP (sans Host — urllib le met automatiquement)
        request_headers = {
            'Content-Type': content_type,
            'Content-Length': str(len(data)),
            'x-amz-content-sha256': payload_hash,
            'x-amz-date': amz_date,
            'Authorization': authorization,
        }
        for meta_key, meta_value in metadata.items():
            request_headers[f'x-amz-meta-{meta_key.lower()}'] = meta_value

        # Requête PUT
        req = urllib.request.Request(
            url,
            data=data,
            headers=request_headers,
            method='PUT',
        )

        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                if resp.status not in (200, 201, 204):
                    body = resp.read().decode('utf-8', errors='replace')
                    raise RuntimeError(
                        f"S3 PUT {resp.status}: {body[:500]}"
                    )
        except urllib.error.HTTPError as e:
            body = e.read().decode('utf-8', errors='replace')
            raise RuntimeError(
                f"S3 PUT {e.code}: {body[:500]}"
            ) from e

    def _get_signing_key(self, date_stamp: str) -> bytes:
        """Dérive la clé de signature AWS4."""
        k_date = self._hmac_sha256(
            f'AWS4{self.secret_access_key}'.encode('utf-8'),
            date_stamp,
        )
        k_region = self._hmac_sha256(k_date, self.region)
        k_service = self._hmac_sha256(k_region, 's3')
        k_signing = self._hmac_sha256(k_service, 'aws4_request')
        return k_signing

    @staticmethod
    def _hmac_sha256(key: bytes, msg: str) -> bytes:
        return hmac.new(key, msg.encode('utf-8'), hashlib.sha256).digest()

    def get_stats(self) -> Dict[str, object]:
        return {
            'total_bytes_uploaded': self.total_bytes_uploaded,
            'total_files_uploaded': self.total_files_uploaded,
            'total_errors': self.total_errors,
            'last_error': self.last_error,
        }