"""Validaciones de seguridad para IBKR Paper Trading."""

import re


def validar_cuentas_paper(cuentas):
    if not isinstance(cuentas, (list, tuple)) or not cuentas:
        return False

    return all(
        isinstance(cuenta, str)
        and re.fullmatch(r"DU(?:R)?[0-9]+", cuenta) is not None
        for cuenta in cuentas
    )


def validar_cuenta_autorizada(cuentas):
    from pathlib import Path

    if not validar_cuentas_paper(cuentas):
        return False

    ruta_env = Path(__file__).resolve().parent.parent / ".env"

    try:
        valores = [
            linea.split("=", 1)[1].strip()
            for linea in ruta_env.read_text(encoding="utf-8").splitlines()
            if linea.startswith("IBKR_PAPER_ACCOUNT=")
        ]
    except (OSError, UnicodeError):
        return False

    if len(valores) != 1 or not valores[0]:
        return False

    return list(cuentas) == valores


def verificar_conexion_paper(ib):
    try:
        return (
            ib.isConnected()
            and validar_cuenta_autorizada(ib.managedAccounts())
        )
    except Exception:
        return False
