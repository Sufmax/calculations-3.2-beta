#!/usr/bin/env python3
"""
bake_all.py — Script Blender (exécution en mode background)

Objectif :
- Forcer Blender à écrire TOUS les caches dans un répertoire unique (--cache-dir)
- Exploiter au maximum les threads CPU via OpenMP (Mantaflow) et mode FIXED
- Supporter le bake parallèle de multiples fluid domains via sous-processus
- Produire un cache_manifest.json pour le cache_streamer

Usage normal :
  blender --background fichier.blend --python bake_all.py -- --cache-dir /path/to/cache

Usage worker (lancé automatiquement pour le bake parallèle multi-domain) :
  blender --background fichier.blend --python bake_all.py -- \\
    --cache-dir /path/to/cache --worker-domain "NomObjet" --worker-threads 23

Compatibilité : Blender 3.2+ (3.6 LTS ciblé), Python 3.8+
Aucun droit superutilisateur requis.
"""

from __future__ import annotations

import argparse
import datetime
import json
import math
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
import traceback
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import bpy

# ─────────────────────────────────────────────
# Constantes
# ─────────────────────────────────────────────

CACHE_SUBDIRS = ("ptcache", "fluids", "rigidbody", "alembic", "geonodes")
CACHE_EXTENSIONS = {".bphys", ".vdb", ".png", ".exr", ".abc", ".obj", ".ply"}

# Threads réservés pour l'OS et le cache_streamer (asyncio + watchdog)
RESERVE_THREADS = 2
# Nombre minimum de threads par worker en mode multi-domain
MIN_THREADS_PER_WORKER = 4
# Nombre maximum de workers parallèles (pour limiter la mémoire)
MAX_PARALLEL_WORKERS = 8

# ─────────────────────────────────────────────
# État global pour gestion des signaux
# ─────────────────────────────────────────────

_interrupted = False
_interrupt_count = 0

# ─────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────

def log(msg: str) -> None:
    print(f"[BAKE_ALL] {msg}", flush=True)

def warn(msg: str) -> None:
    print(f"[BAKE_ALL][WARN] {msg}", flush=True)

def err(msg: str) -> None:
    print(f"[BAKE_ALL][ERROR] {msg}", flush=True)

# ─────────────────────────────────────────────
# Gestion des signaux
# ─────────────────────────────────────────────

def _signal_handler(signum: int, frame: Any) -> None:
    global _interrupted, _interrupt_count
    _interrupted = True
    _interrupt_count += 1
    sig_name = signal.Signals(signum).name if hasattr(signal, "Signals") else str(signum)
    warn(f"Signal {sig_name} reçu (#{_interrupt_count})")
    if _interrupt_count >= 3:
        err("3 interruptions → arrêt immédiat")
        sys.exit(1)

def install_signal_handlers() -> None:
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            signal.signal(sig, _signal_handler)
        except (OSError, ValueError):
            pass

# ─────────────────────────────────────────────
# Parsing d'arguments CLI
# ─────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    argv = sys.argv
    if "--" in argv:
        argv = argv[argv.index("--") + 1:]
    else:
        argv = []

    parser = argparse.ArgumentParser(description="Blender bake-all helper")

    parser.add_argument("--cache-dir", required=True,
                        help="Répertoire racine des caches")
    parser.add_argument("--frame-start", type=int, default=None)
    parser.add_argument("--frame-end", type=int, default=None)
    parser.add_argument("--clear-existing", action="store_true")

    # Toggles simulation (activés par défaut)
    parser.add_argument("--bake-fluids", action="store_true", default=True)
    parser.add_argument("--no-bake-fluids", dest="bake_fluids", action="store_false")
    parser.add_argument("--bake-particles", action="store_true", default=True)
    parser.add_argument("--no-bake-particles", dest="bake_particles", action="store_false")
    parser.add_argument("--bake-cloth", action="store_true", default=True)
    parser.add_argument("--no-bake-cloth", dest="bake_cloth", action="store_false")

    # Threading
    parser.add_argument("--bake-threads", type=int, default=None,
                        help="Nombre de threads pour le bake (défaut: cpu_count - 2)")

    # Mode worker (utilisé en interne pour le bake parallèle multi-domain)
    parser.add_argument("--worker-domain", type=str, default=None,
                        help="Mode worker : nom de l'objet fluid domain à baker")
    parser.add_argument("--worker-threads", type=int, default=None,
                        help="Mode worker : threads alloués à ce worker")

    # Modes généraux
    parser.add_argument("--strict", action="store_true")
    parser.add_argument("--all-scenes", action="store_true")
    parser.add_argument("--verbose", action="store_true")

    return parser.parse_args(argv)

# ─────────────────────────────────────────────
# Vérifications et initialisation
# ─────────────────────────────────────────────

def verify_blend_loaded() -> bool:
    if not bpy.data.filepath:
        err("Aucun fichier .blend chargé")
        return False
    log(f"Fichier .blend chargé : {bpy.data.filepath}")
    return True

def setup_cache_directories(cache_root: Path) -> Dict[str, Path]:
    dirs: Dict[str, Path] = {}
    for name in CACHE_SUBDIRS:
        d = cache_root / name
        d.mkdir(parents=True, exist_ok=True)
        dirs[name] = d
    return dirs

# ─────────────────────────────────────────────
# Redirection ptcache via symlink
# ─────────────────────────────────────────────

def setup_ptcache_symlink(cache_root: Path, verbose: bool = False) -> bool:
    blend_path = Path(bpy.data.filepath)
    if not blend_path.exists():
        warn("Fichier .blend introuvable, symlink impossible")
        return False

    blendcache_dir = blend_path.parent / f"blendcache_{blend_path.stem}"
    target = cache_root / "ptcache"

    if blendcache_dir.is_symlink():
        try:
            if blendcache_dir.resolve() == target.resolve():
                if verbose:
                    log(f"Symlink déjà correct : {blendcache_dir} → {target}")
                return True
        except OSError:
            pass
        try:
            blendcache_dir.unlink()
        except OSError as e:
            warn(f"Impossible de supprimer l'ancien symlink : {e}")
            return False

    if blendcache_dir.is_dir() and not blendcache_dir.is_symlink():
        if verbose:
            log(f"Migration {blendcache_dir} → {target}")
        try:
            for f in blendcache_dir.rglob("*"):
                if f.is_file():
                    dest = target / f.relative_to(blendcache_dir)
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(str(f), str(dest))
            shutil.rmtree(str(blendcache_dir), ignore_errors=True)
        except OSError as e:
            warn(f"Erreur migration blendcache : {e}")
            return False

    if blendcache_dir.exists() and not blendcache_dir.is_dir():
        try:
            blendcache_dir.unlink()
        except OSError:
            return False

    try:
        blendcache_dir.symlink_to(target, target_is_directory=True)
        log(f"Symlink créé : {blendcache_dir} → {target}")
        return True
    except OSError as e:
        warn(f"Échec symlink : {e}")
        return False

# ─────────────────────────────────────────────
# Configuration du threading (POINT CLÉ)
# ─────────────────────────────────────────────

def configure_threading(
    scene: bpy.types.Scene,
    n_threads: int,
    verbose: bool = False,
) -> Dict[str, Any]:
    """Configure OpenMP + Blender pour utiliser n_threads cœurs.

    Mantaflow utilise OpenMP (#pragma omp parallel for) pour
    paralléliser le solveur fluide (advection, pression, bruit).
    Le nombre de threads est déterminé par :
      1. OMP_NUM_THREADS (variable d'environnement)
      2. BLI_system_thread_count() → lit render.threads en mode FIXED

    CRITIQUE : en mode AUTO (le défaut), Blender peut détecter 1 seul
    cœur en background sur une VM → le solveur tourne en mono-thread.
    Le mode FIXED force le nombre exact.
    """
    report: Dict[str, Any] = {
        "requested": n_threads,
        "cpu_count": os.cpu_count() or 1,
    }

    # Variable d'environnement OpenMP (lue avant le premier parallel region)
    os.environ["OMP_NUM_THREADS"] = str(n_threads)
    # Pas de binding : laisser le scheduler Linux répartir
    os.environ.pop("OMP_PROC_BIND", None)
    report["omp_num_threads"] = n_threads

    # Blender render threads en mode FIXED
    try:
        scene.render.threads_mode = "FIXED"
        scene.render.threads = n_threads
        report["render_threads_mode"] = "FIXED"
        report["render_threads"] = n_threads
    except Exception as e:
        report["render_error"] = str(e)

    # Préférences système (redondant mais garantit la cohérence)
    try:
        prefs = bpy.context.preferences
        if hasattr(prefs, "system"):
            prefs.system.threads = n_threads
            report["prefs_threads"] = n_threads
    except Exception:
        pass

    if verbose:
        log(f"Threading : {n_threads} threads, mode=FIXED, "
            f"OMP_NUM_THREADS={n_threads}")

    return report

# ─────────────────────────────────────────────
# Configuration des chemins de cache
# ─────────────────────────────────────────────

def configure_fluid_domains(
    scene: bpy.types.Scene,
    fluids_dir: Path,
    verbose: bool = False,
) -> int:
    count = 0
    for obj in scene.objects:
        for mod in obj.modifiers:
            if mod.type != "FLUID":
                continue
            if getattr(mod, "fluid_type", None) != "DOMAIN":
                continue
            ds = getattr(mod, "domain_settings", None)
            if ds is None:
                continue
            try:
                ds.cache_directory = str(fluids_dir)
                if hasattr(ds, "cache_data_format"):
                    ds.cache_data_format = "OPENVDB"
                if hasattr(ds, "openvdb_cache_compress_type"):
                    ds.openvdb_cache_compress_type = "BLOSC"
                count += 1
                if verbose:
                    log(f"Fluid domain '{obj.name}' → {fluids_dir} (OpenVDB)")
            except Exception as e:
                warn(f"Erreur config fluid domain '{obj.name}' : {e}")
    return count

def configure_disk_caches(
    scene: bpy.types.Scene,
    verbose: bool = False,
) -> int:
    count = 0

    def _configure_pc(pc: Any, label: str) -> bool:
        nonlocal count
        try:
            if hasattr(pc, "use_disk_cache"):
                pc.use_disk_cache = True
            if hasattr(pc, "use_external"):
                pc.use_external = False
            if hasattr(pc, "use_library_path"):
                pc.use_library_path = False
            count += 1
            if verbose:
                log(f"  Disk cache activé : {label}")
            return True
        except Exception:
            return False

    rbw = getattr(scene, "rigidbody_world", None)
    if rbw is not None:
        pc = getattr(rbw, "point_cache", None)
        if pc is not None:
            _configure_pc(pc, "RigidBodyWorld")

    for obj in scene.objects:
        for i, psys in enumerate(getattr(obj, "particle_systems", [])):
            pc = getattr(psys, "point_cache", None)
            if pc is not None:
                _configure_pc(pc, f"{obj.name}/particles[{i}]")

        for mod in obj.modifiers:
            if mod.type in ("CLOTH", "SOFT_BODY"):
                pc = getattr(mod, "point_cache", None)
                if pc is not None:
                    _configure_pc(pc, f"{obj.name}/{mod.name}")
            elif mod.type == "DYNAMIC_PAINT":
                canvas = getattr(mod, "canvas_settings", None)
                if canvas is not None and hasattr(canvas, "canvas_surfaces"):
                    for j, surf in enumerate(canvas.canvas_surfaces):
                        pc = getattr(surf, "point_cache", None)
                        if pc is not None:
                            _configure_pc(pc, f"{obj.name}/DynPaint[{j}]")
            elif mod.type not in ("FLUID",):
                pc = getattr(mod, "point_cache", None)
                if pc is not None:
                    _configure_pc(pc, f"{obj.name}/{mod.name}")

    if verbose:
        log(f"Total caches disque configurés : {count}")
    return count

# ─────────────────────────────────────────────
# Optimisation performances (sans root)
# ─────────────────────────────────────────────

def optimize_performance(
    scene: bpy.types.Scene,
    n_threads: int,
    verbose: bool = False,
) -> Dict[str, Any]:
    """Configure threading + affinité CPU + I/O priority."""
    report: Dict[str, Any] = {"cpu_count": os.cpu_count() or 1}

    # Threading OpenMP + Blender (le fix principal)
    threading_report = configure_threading(scene, n_threads, verbose)
    report.update(threading_report)

    # Affinité CPU → tous les cœurs
    try:
        if hasattr(os, "sched_setaffinity"):
            cpu_count = os.cpu_count() or 1
            os.sched_setaffinity(0, set(range(cpu_count)))
            report["affinity"] = f"all({cpu_count})"
        else:
            report["affinity"] = "not_supported"
    except Exception as e:
        report["affinity"] = f"error: {e}"

    # I/O priority
    try:
        pid = str(os.getpid())
        res = subprocess.run(
            ["ionice", "-c2", "-n0", "-p", pid],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, check=False, timeout=5,
        )
        report["ionice"] = "ok" if res.returncode == 0 else f"failed({res.returncode})"
    except FileNotFoundError:
        report["ionice"] = "not_installed"
    except Exception as e:
        report["ionice"] = f"error: {e}"

    return report

# ─────────────────────────────────────────────
# Utilitaires de contexte Blender
# ─────────────────────────────────────────────

def activate_scene(scene: bpy.types.Scene) -> None:
    try:
        window = bpy.context.window
        if window is not None:
            window.scene = scene
    except Exception:
        pass
    try:
        bpy.context.view_layer.update()
    except Exception:
        pass

def activate_object(scene: bpy.types.Scene, obj: bpy.types.Object) -> None:
    activate_scene(scene)
    try:
        for o in bpy.context.view_layer.objects:
            o.select_set(False)
        obj.select_set(True)
        bpy.context.view_layer.objects.active = obj
    except Exception:
        pass

# ─────────────────────────────────────────────
# Nettoyage des caches
# ─────────────────────────────────────────────

def clear_all_caches(scene: bpy.types.Scene, verbose: bool = False) -> None:
    activate_scene(scene)
    try:
        bpy.ops.ptcache.free_bake_all()
        if verbose:
            log("ptcache.free_bake_all → OK")
    except Exception as e:
        if verbose:
            warn(f"ptcache.free_bake_all échoué : {e}")

    for obj in scene.objects:
        for mod in obj.modifiers:
            if mod.type == "FLUID" and getattr(mod, "fluid_type", None) == "DOMAIN":
                try:
                    activate_object(scene, obj)
                    bpy.ops.fluid.free_all()
                    if verbose:
                        log(f"fluid.free_all → OK ({obj.name})")
                except Exception as e:
                    if verbose:
                        warn(f"fluid.free_all échoué ({obj.name}) : {e}")

# ─────────────────────────────────────────────
# Bake : Point Caches
# ─────────────────────────────────────────────

def bake_point_caches(
    scene: bpy.types.Scene,
    verbose: bool = False,
) -> Dict[str, Any]:
    result: Dict[str, Any] = {"baked": 0, "errors": [], "ok": False, "method": "none"}

    if _interrupted:
        result["errors"].append("Interrompu avant bake ptcache")
        return result

    activate_scene(scene)

    # Tentative globale
    try:
        bpy.ops.ptcache.bake_all(bake=True)
        result["baked"] = 1
        result["ok"] = True
        result["method"] = "ptcache.bake_all"
        if verbose:
            log("ptcache.bake_all(bake=True) → OK")
        return result
    except Exception as e:
        if verbose:
            warn(f"ptcache.bake_all échoué : {e}, fallback individuel")

    # Fallback individuel
    result["method"] = "individual"
    errors: List[str] = []
    baked = 0

    for obj in scene.objects:
        has_physics = False
        for mod in obj.modifiers:
            if mod.type in ("CLOTH", "SOFT_BODY", "DYNAMIC_PAINT"):
                has_physics = True
                break
        if not has_physics and len(getattr(obj, "particle_systems", [])) == 0:
            continue
        if _interrupted:
            errors.append("Interrompu")
            break
        try:
            activate_object(scene, obj)
            bpy.ops.ptcache.bake_all(bake=True)
            baked += 1
            if verbose:
                log(f"  ptcache OK : {obj.name}")
        except Exception as e:
            errors.append(f"ptcache '{obj.name}' : {e}")
        finally:
            try:
                obj.select_set(False)
            except Exception:
                pass

    result["baked"] = baked
    result["errors"] = errors
    result["ok"] = baked > 0 or len(errors) == 0
    return result

# ─────────────────────────────────────────────
# Bake : Fluid Domains — séquentiel
# ─────────────────────────────────────────────

def bake_fluid_domains(
    scene: bpy.types.Scene,
    verbose: bool = False,
) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "domains_found": 0, "baked": 0, "errors": [], "ok": False,
    }
    activate_scene(scene)

    for obj in scene.objects:
        for mod in obj.modifiers:
            if mod.type != "FLUID" or getattr(mod, "fluid_type", None) != "DOMAIN":
                continue
            result["domains_found"] += 1
            if _interrupted:
                result["errors"].append("Interrompu")
                return result
            try:
                activate_object(scene, obj)
                bpy.ops.fluid.bake_all()
                result["baked"] += 1
                if verbose:
                    log(f"  fluid.bake_all → OK ({obj.name})")
            except Exception as e:
                result["errors"].append(f"fluid '{obj.name}' : {e}")
                warn(f"fluid bake '{obj.name}' : {e}")
            finally:
                try:
                    obj.select_set(False)
                except Exception:
                    pass

    result["ok"] = (
        result["baked"] == result["domains_found"]
        or (result["domains_found"] == 0 and not result["errors"])
    )
    return result

# ─────────────────────────────────────────────
# Bake : Fluid Domains — parallèle multi-domain
# ─────────────────────────────────────────────

def _find_fluid_domains(scene: bpy.types.Scene) -> List[str]:
    """Retourne la liste des noms d'objets fluid domain dans la scène."""
    domains: List[str] = []
    for obj in scene.objects:
        for mod in obj.modifiers:
            if mod.type == "FLUID" and getattr(mod, "fluid_type", None) == "DOMAIN":
                domains.append(obj.name)
                break
    return domains


def _stream_subprocess_output(
    proc: subprocess.Popen,
    label: str,
) -> None:
    """Thread daemon : lit stdout d'un worker et le relaie dans nos logs."""
    try:
        for raw_line in proc.stdout:
            line = raw_line.rstrip("\n").rstrip("\r")
            if line:
                log(f"[{label}] {line}")
    except Exception:
        pass


def parallel_bake_fluid_domains(
    scene: bpy.types.Scene,
    cache_root: Path,
    domains: List[str],
    n_total_threads: int,
    args: argparse.Namespace,
) -> Optional[Dict[str, Any]]:
    """Bake les fluid domains en parallèle via sous-processus Blender.

    Chaque worker :
    - Ouvre le même .blend
    - Configure son domain avec cache_directory = fluids/<domain_name>/
    - Configure ses threads (n_total_threads / n_workers)
    - Bake avec fluid.bake_all()

    Retourne un dict compatible avec bake_fluid_domains(), ou None si
    le parallélisme n'est pas applicable (< 2 domains).
    """
    n_domains = len(domains)
    if n_domains < 2:
        return None  # Utiliser le mode séquentiel

    # Limiter le nombre de workers
    n_workers = min(n_domains, MAX_PARALLEL_WORKERS)
    if n_workers < n_domains:
        warn(f"Trop de domains ({n_domains}), limité à {n_workers} workers")
        # On ne bake que les N premiers en parallèle ; les autres en séquentiel
        # Pour simplifier, on bake tous en parallèle jusqu'à MAX
        domains = domains[:n_workers]
        n_domains = n_workers

    # Calcul des threads par worker
    threads_per_worker = max(MIN_THREADS_PER_WORKER, n_total_threads // n_domains)
    log(f"Bake parallèle : {n_domains} domains × {threads_per_worker} threads")

    # Chemins
    blender_exe = bpy.app.binary_path
    blend_file = bpy.data.filepath
    script_path = os.path.abspath(__file__)

    result: Dict[str, Any] = {
        "domains_found": n_domains,
        "baked": 0,
        "errors": [],
        "ok": False,
        "method": "parallel",
    }

    # Lancer les workers
    workers: List[Tuple[str, subprocess.Popen]] = []
    reader_threads: List[threading.Thread] = []

    for domain_name in domains:
        if _interrupted:
            result["errors"].append("Interrompu avant lancement des workers")
            break

        # Créer le sous-dossier pour ce domain
        domain_cache = cache_root / "fluids" / domain_name
        domain_cache.mkdir(parents=True, exist_ok=True)

        # Commande worker
        cmd = [
            blender_exe,
            "--background",
            blend_file,
            "--python", script_path,
            "--",
            "--cache-dir", str(cache_root),
            "--worker-domain", domain_name,
            "--worker-threads", str(threads_per_worker),
        ]
        if args.frame_start is not None:
            cmd.extend(["--frame-start", str(args.frame_start)])
        if args.frame_end is not None:
            cmd.extend(["--frame-end", str(args.frame_end)])
        if args.verbose:
            cmd.append("--verbose")

        # Environnement avec OMP_NUM_THREADS pour ce worker
        env = os.environ.copy()
        env["OMP_NUM_THREADS"] = str(threads_per_worker)

        log(f"Lancement worker '{domain_name}' ({threads_per_worker} threads)")

        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                env=env,
            )
            workers.append((domain_name, proc))

            # Thread pour lire la sortie du worker
            t = threading.Thread(
                target=_stream_subprocess_output,
                args=(proc, f"WORKER-{domain_name}"),
                daemon=True,
            )
            t.start()
            reader_threads.append(t)

        except Exception as e:
            result["errors"].append(f"Lancement worker '{domain_name}' : {e}")
            err(f"Impossible de lancer le worker '{domain_name}' : {e}")

    # Attendre la fin de tous les workers
    for domain_name, proc in workers:
        try:
            # Polling avec vérification d'interruption
            while proc.poll() is None:
                if _interrupted:
                    warn(f"Interruption → arrêt worker '{domain_name}'")
                    proc.terminate()
                    try:
                        proc.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                    break
                time.sleep(0.5)

            rc = proc.returncode
            if rc == 0:
                result["baked"] += 1
                log(f"Worker '{domain_name}' terminé (code 0)")
            else:
                result["errors"].append(
                    f"Worker '{domain_name}' code={rc}"
                )
                warn(f"Worker '{domain_name}' terminé avec code {rc}")

        except Exception as e:
            result["errors"].append(f"Worker '{domain_name}' : {e}")

    # Attendre les threads de lecture
    for t in reader_threads:
        t.join(timeout=5)

    result["ok"] = result["baked"] == result["domains_found"]
    return result

# ─────────────────────────────────────────────
# Mode Worker : bake un seul fluid domain
# ─────────────────────────────────────────────

def _worker_bake_domain(args: argparse.Namespace) -> int:
    """Point d'entrée mode worker : bake un seul fluid domain.

    Lancé comme sous-processus par parallel_bake_fluid_domains().
    Configure ses propres threads et cache, bake, et sort.
    """
    install_signal_handlers()

    domain_name = args.worker_domain
    n_threads = args.worker_threads or max(1, (os.cpu_count() or 1) - RESERVE_THREADS)
    cache_root = Path(args.cache_dir).expanduser().resolve()

    log(f"Worker démarré : domain='{domain_name}', threads={n_threads}")

    # Vérifier le .blend
    if not bpy.data.filepath:
        err("Aucun .blend chargé")
        return 1

    scene = bpy.context.scene

    # Configurer le threading
    configure_threading(scene, n_threads, verbose=args.verbose)

    # Override frame range
    if args.frame_start is not None:
        scene.frame_start = args.frame_start
    if args.frame_end is not None:
        scene.frame_end = args.frame_end

    # Trouver l'objet domain
    obj = scene.objects.get(domain_name)
    if obj is None:
        err(f"Objet '{domain_name}' introuvable dans la scène")
        return 1

    # Trouver le modifier fluid domain
    fluid_mod = None
    for mod in obj.modifiers:
        if mod.type == "FLUID" and getattr(mod, "fluid_type", None) == "DOMAIN":
            fluid_mod = mod
            break

    if fluid_mod is None:
        err(f"Pas de fluid domain modifier sur '{domain_name}'")
        return 1

    # Configurer le cache pour CE domain
    domain_cache = cache_root / "fluids" / domain_name
    domain_cache.mkdir(parents=True, exist_ok=True)

    ds = fluid_mod.domain_settings
    ds.cache_directory = str(domain_cache)
    if hasattr(ds, "cache_data_format"):
        ds.cache_data_format = "OPENVDB"
    if hasattr(ds, "openvdb_cache_compress_type"):
        ds.openvdb_cache_compress_type = "BLOSC"

    log(f"Cache domain → {domain_cache}")
    log(f"Frames : {scene.frame_start} → {scene.frame_end}")

    # Activer l'objet et bake
    activate_object(scene, obj)

    start = time.time()
    try:
        bpy.ops.fluid.bake_all()
        duration = round(time.time() - start, 2)
        log(f"Fluid bake '{domain_name}' OK en {duration}s")
        return 0
    except Exception as e:
        err(f"Fluid bake '{domain_name}' échoué : {e}")
        if args.verbose:
            traceback.print_exc()
        return 1

# ─────────────────────────────────────────────
# Récupération de fichiers orphelins
# ─────────────────────────────────────────────

def collect_orphan_caches(cache_root: Path, verbose: bool = False) -> int:
    blend_path = Path(bpy.data.filepath)
    if not blend_path.exists():
        return 0
    blendcache_dir = blend_path.parent / f"blendcache_{blend_path.stem}"
    target = cache_root / "ptcache"
    if blendcache_dir.is_symlink() or not blendcache_dir.is_dir():
        return 0

    count = 0
    for f in blendcache_dir.rglob("*"):
        if not f.is_file():
            continue
        try:
            dest = target / f.relative_to(blendcache_dir)
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(str(f), str(dest))
            count += 1
        except Exception as e:
            if verbose:
                warn(f"Erreur copie orpheline {f} : {e}")

    if count > 0:
        log(f"{count} fichiers ptcache orphelins récupérés → {target}")
    return count

# ─────────────────────────────────────────────
# Manifest de cache
# ─────────────────────────────────────────────

def count_cache_files(cache_root: Path) -> Tuple[int, int]:
    file_count = 0
    total_bytes = 0
    for f in cache_root.rglob("*"):
        if f.is_file() and f.name != "cache_manifest.json":
            try:
                total_bytes += f.stat().st_size
                file_count += 1
            except OSError:
                pass
    return file_count, total_bytes


def write_manifest(
    cache_root: Path,
    scenes_info: List[Dict[str, Any]],
    status: str,
    frame_start: Optional[int] = None,
    frame_end: Optional[int] = None,
) -> Path:
    if scenes_info:
        scene_name = scenes_info[0].get("scene", "unknown")
        if len(scenes_info) > 1:
            scene_name = ", ".join(s.get("scene", "?") for s in scenes_info)
    else:
        scene_name = "unknown"

    if frame_start is None or frame_end is None:
        starts = [s.get("frame_start", 1) for s in scenes_info if "frame_start" in s]
        ends = [s.get("frame_end", 250) for s in scenes_info if "frame_end" in s]
        frame_start = min(starts) if starts else 1
        frame_end = max(ends) if ends else 250

    files: List[Dict[str, Any]] = []
    for f in cache_root.rglob("*"):
        if f.is_file() and f.name != "cache_manifest.json":
            try:
                stat = f.stat()
                files.append({
                    "path": str(f.relative_to(cache_root)),
                    "size": stat.st_size,
                    "timestamp": datetime.datetime.fromtimestamp(
                        stat.st_mtime, tz=datetime.timezone.utc
                    ).isoformat(),
                })
            except OSError:
                pass

    manifest = {
        "blender_version": bpy.app.version_string,
        "scene": scene_name,
        "frame_range": [frame_start, frame_end],
        "cache_dir": str(cache_root),
        "timestamp": datetime.datetime.now(tz=datetime.timezone.utc).isoformat(),
        "files": files,
        "status": status,
        "scenes_detail": scenes_info,
    }

    manifest_path = cache_root / "cache_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest_path

# ─────────────────────────────────────────────
# Point d'entrée principal
# ─────────────────────────────────────────────

def main() -> int:
    install_signal_handlers()
    args = parse_args()

    # ── Mode worker : bake un seul domain et sortir ──
    if args.worker_domain is not None:
        return _worker_bake_domain(args)

    # ── Mode normal : orchestration complète ──
    cache_root = Path(args.cache_dir).expanduser().resolve()
    cache_root.mkdir(parents=True, exist_ok=True)

    # Calcul du nombre de threads pour le bake
    cpu_count = os.cpu_count() or 1
    n_threads = args.bake_threads if args.bake_threads else max(1, cpu_count - RESERVE_THREADS)

    # Bannière
    log("=" * 70)
    log("Démarrage bake_all.py")
    log(f"  Blender       : {bpy.app.version_string}")
    log(f"  Fichier .blend: {bpy.data.filepath or '(aucun)'}")
    log(f"  Cache dir     : {cache_root}")
    log(f"  CPU           : {cpu_count} threads disponibles")
    log(f"  Bake threads  : {n_threads} (réserve: {RESERVE_THREADS})")
    log(f"  Bake fluids   : {args.bake_fluids}")
    log(f"  Bake particles: {args.bake_particles}")
    log(f"  Bake cloth    : {args.bake_cloth}")
    log(f"  Clear existing: {args.clear_existing}")
    log(f"  Strict        : {args.strict}")
    log(f"  All scenes    : {args.all_scenes}")
    log("=" * 70)

    start_time = time.time()

    if not verify_blend_loaded():
        write_manifest(cache_root, [], "error")
        return 1

    cache_dirs = setup_cache_directories(cache_root)
    log(f"Sous-dossiers créés : {', '.join(CACHE_SUBDIRS)}")

    symlink_ok = setup_ptcache_symlink(cache_root, verbose=args.verbose)
    if not symlink_ok:
        warn("Symlink ptcache non disponible — récupération post-bake")

    if args.all_scenes:
        scenes = list(bpy.data.scenes)
    else:
        scenes = [bpy.context.scene]

    log(f"Scènes à baker : {', '.join(s.name for s in scenes)}")

    # Optimisation (threading + affinité + I/O)
    perf_report = optimize_performance(scenes[0], n_threads, verbose=args.verbose)
    log(f"Performance : OMP_NUM_THREADS={n_threads}, mode=FIXED")
    if args.verbose:
        log(f"Rapport perf : {perf_report}")

    # ── Boucle par scène ──
    scenes_info: List[Dict[str, Any]] = []
    all_errors: List[str] = []
    total_ptcache_baked = 0
    total_fluid_baked = 0

    for scene in scenes:
        if _interrupted:
            warn("Interruption détectée entre les scènes")
            break

        scene_result: Dict[str, Any] = {
            "scene": scene.name,
            "ok": True,
            "errors": [],
            "ptcache": {},
            "fluids": {},
        }

        try:
            activate_scene(scene)

            # Appliquer le threading à chaque scène
            configure_threading(scene, n_threads, verbose=args.verbose)

            if args.frame_start is not None:
                scene.frame_start = args.frame_start
            if args.frame_end is not None:
                scene.frame_end = args.frame_end

            scene_result["frame_start"] = scene.frame_start
            scene_result["frame_end"] = scene.frame_end
            log(f"[{scene.name}] Frames : {scene.frame_start} → {scene.frame_end}")

            n_caches = configure_disk_caches(scene, verbose=args.verbose)
            scene_result["disk_caches_configured"] = n_caches
            log(f"[{scene.name}] {n_caches} point caches configurés")

            if args.bake_fluids:
                n_fluids = configure_fluid_domains(
                    scene, cache_dirs["fluids"], verbose=args.verbose,
                )
                scene_result["fluid_domains_configured"] = n_fluids
                if n_fluids > 0:
                    log(f"[{scene.name}] {n_fluids} fluid domains → {cache_dirs['fluids']}")

            if args.clear_existing:
                log(f"[{scene.name}] Nettoyage caches existants…")
                clear_all_caches(scene, verbose=args.verbose)

            # ── Bake point caches ──
            if args.bake_cloth or args.bake_particles:
                log(f"[{scene.name}] Bake point caches…")
                ptcache_result = bake_point_caches(scene, verbose=args.verbose)
                scene_result["ptcache"] = ptcache_result
                total_ptcache_baked += ptcache_result.get("baked", 0)

                if ptcache_result.get("ok"):
                    log(f"[{scene.name}] Point caches OK "
                        f"(méthode={ptcache_result.get('method')}, "
                        f"baked={ptcache_result.get('baked')})")
                else:
                    for e in ptcache_result.get("errors", []):
                        scene_result["errors"].append(e)
                    warn(f"[{scene.name}] Point caches : erreurs")
            else:
                log(f"[{scene.name}] Bake cloth/particles désactivé")

            # ── Bake fluids ──
            if _interrupted:
                scene_result["errors"].append("Interrompu avant fluids")
            elif args.bake_fluids:
                log(f"[{scene.name}] Bake fluid domains…")

                # Déterminer si parallélisme multi-domain possible
                domains = _find_fluid_domains(scene)
                fluid_result = None

                if len(domains) >= 2:
                    log(f"[{scene.name}] {len(domains)} domains → mode parallèle")
                    fluid_result = parallel_bake_fluid_domains(
                        scene, cache_root, domains, n_threads, args,
                    )

                if fluid_result is None:
                    # Mode séquentiel (1 domain ou parallèle non applicable)
                    fluid_result = bake_fluid_domains(scene, verbose=args.verbose)

                scene_result["fluids"] = fluid_result
                total_fluid_baked += fluid_result.get("baked", 0)

                if fluid_result.get("domains_found", 0) > 0:
                    if fluid_result.get("ok"):
                        log(f"[{scene.name}] Fluids OK "
                            f"(baked={fluid_result.get('baked')}/"
                            f"{fluid_result.get('domains_found')})")
                    else:
                        for e in fluid_result.get("errors", []):
                            scene_result["errors"].append(e)
                        warn(f"[{scene.name}] Fluids : erreurs")
                else:
                    if args.verbose:
                        log(f"[{scene.name}] Aucun fluid domain")
            else:
                log(f"[{scene.name}] Bake fluids désactivé")

            # Récupération orphelins
            if not symlink_ok:
                n_orphans = collect_orphan_caches(cache_root, verbose=args.verbose)
                if n_orphans > 0:
                    scene_result["orphans_recovered"] = n_orphans

        except Exception as e:
            scene_result["ok"] = False
            scene_result["errors"].append(str(e))
            err(f"[{scene.name}] Erreur critique : {e}")
            if args.verbose:
                traceback.print_exc()

        if scene_result["errors"]:
            scene_result["ok"] = False
            all_errors.extend(scene_result["errors"])

        scenes_info.append(scene_result)

        if args.strict and not scene_result["ok"]:
            err(f"[{scene.name}] Mode strict → arrêt")
            break

    # ── Post-bake ──
    duration = round(time.time() - start_time, 2)
    file_count, total_bytes = count_cache_files(cache_root)

    if _interrupted:
        status = "interrupted"
    elif not all_errors:
        status = "complete"
    else:
        any_success = any(s.get("ok") for s in scenes_info)
        status = "partial" if any_success else "error"

    manifest_path = write_manifest(
        cache_root, scenes_info, status,
        frame_start=args.frame_start, frame_end=args.frame_end,
    )
    log(f"Manifest écrit : {manifest_path}")

    # Résumé
    log("=" * 70)
    log(f"RÉSUMÉ — statut: {status.upper()}")
    log(f"  Durée           : {duration}s")
    log(f"  Threads bake    : {n_threads}")
    log(f"  Scènes traitées : {len(scenes_info)}")
    log(f"  Point caches    : {total_ptcache_baked} baked")
    log(f"  Fluid domains   : {total_fluid_baked} baked")
    log(f"  Fichiers cache  : {file_count} ({total_bytes} bytes)")
    log(f"  Erreurs         : {len(all_errors)}")
    if all_errors:
        for i, e in enumerate(all_errors[:10], 1):
            err(f"  [{i}] {e}")
        if len(all_errors) > 10:
            err(f"  … et {len(all_errors) - 10} autres")
    log("=" * 70)

    if _interrupted:
        return 1
    if not all_errors:
        log("BAKE COMPLET — succès")
        return 0
    if args.strict:
        return 1
    return 2


if __name__ == "__main__":
    raise SystemExit(main())