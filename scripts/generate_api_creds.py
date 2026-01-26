import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from py_clob_client.client import ClobClient


def _load_env() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    backend_env = repo_root / "backend" / ".env"
    if backend_env.exists():
        load_dotenv(backend_env)
        return
    load_dotenv()


def _required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        print(f"Missing required env var: {name}")
        sys.exit(1)
    return value


def main() -> None:
    _load_env()

    host = os.getenv("POLYMARKET_CLOB_HOST", "https://clob.polymarket.com").strip()
    chain_id = int(os.getenv("POLYMARKET_CHAIN_ID", "137"))
    private_key = _required("POLYMARKET_PRIVATE_KEY")

    signature_type_raw = os.getenv("POLYMARKET_SIGNATURE_TYPE", "").strip()
    signature_type = int(signature_type_raw) if signature_type_raw else None

    funder = os.getenv("POLYMARKET_FUNDER_ADDRESS", "").strip()
    if not funder:
        funder = os.getenv("POLYMARKET_USER_ADDRESS", "").strip() or None

    client = ClobClient(
        host,
        chain_id=chain_id,
        key=private_key,
        signature_type=signature_type,
        funder=funder,
    )
    creds = client.create_or_derive_api_creds()

    print("\nGenerated Polymarket API credentials (store these in backend/.env):\n")
    print(f"POLYMARKET_API_KEY={creds.api_key}")
    print(f"POLYMARKET_API_SECRET={creds.api_secret}")
    print(f"POLYMARKET_API_PASSPHRASE={creds.api_passphrase}")
    print("\nNote: These cannot be recovered if lost. Store securely.")


if __name__ == "__main__":
    main()
