"""Procura padrões SQL que costumam falhar ao alternar SQLite/PostgreSQL."""
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
TARGETS = [ROOT / 'modules', ROOT / 'services', ROOT / 'repositories', ROOT / 'database.py']
PATTERNS = {
    'alias_day_sem_as': re.compile(r'\bSELECT\b[^\n]*\)\s+day\s*,', re.I),
    'order_by_day_sem_aspas': re.compile(r'\bORDER\s+BY\s+day\b', re.I),
    'sqlite_sql_strftime': re.compile(r'\bstrftime\s*\(', re.I),
    'sqlite_sql_datetime_now': re.compile(r'\bdatetime\s*\(\s*["\']now["\']', re.I),
}
SQL_HINTS = ('SELECT ', 'WHERE ', 'ORDER BY ', 'GROUP BY ', 'INSERT ', 'UPDATE ', 'DELETE ', 'FROM ')

findings = []
for target in TARGETS:
    files = [target] if target.is_file() else target.rglob('*.py')
    for path in files:
        text = path.read_text(encoding='utf-8', errors='replace')
        for line_no, line in enumerate(text.splitlines(), 1):
            upper = line.upper()
            for name, pattern in PATTERNS.items():
                if name.startswith('sqlite_sql_') and not any(hint in upper for hint in SQL_HINTS):
                    continue
                if pattern.search(line):
                    findings.append((path.relative_to(ROOT), line_no, name, line.strip()))

if findings:
    print('Possíveis incompatibilidades encontradas:')
    for path, line_no, name, line in findings:
        print(f'- {path}:{line_no} [{name}] {line}')
    raise SystemExit(1)

print('Nenhum padrão SQL crítico conhecido foi encontrado.')
