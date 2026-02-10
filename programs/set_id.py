import os
import re

def get():
    user = os.environ.get("JUPYTERHUB_USER", "")
    # attendu: freechipsproject-chisel-bootcamp-hy0ibf9s

    m = re.search(r"-([a-z0-9]+)$", user)
    if not m:
        raise RuntimeError("Identifiant Binder introuvable")

    binder_id = m.group(1)
    return binder_id


if __name__ == "__main__":
    from dotenv import set_key
    set_key(".env", "VM_PASSWORD", get())
    print(get())
