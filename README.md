1. Install Uv:
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```
2. Install dependencies:
```bash
uv sync 
```
3. Activate venv:
```bash
source .venv/bin/activate
```
4. Run the script:
```bash
uv run import_btc.py 
```

The script will output a `imported_vaults.csv` file with each vault's name, derivation path and corresponding Fordefi Vault ID.

