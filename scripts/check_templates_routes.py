from __future__ import annotations

import ast
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
URL_FOR_RE = re.compile(r"url_for\(\s*['\"]([^'\"]+)['\"]")


def _string(node):
    return node.value if isinstance(node, ast.Constant) and isinstance(node.value, str) else None


def collect_endpoints() -> set[str]:
    endpoints = {"static"}
    for py_file in [ROOT / "main.py", *sorted((ROOT / "modules").rglob("*.py"))]:
        try:
            tree = ast.parse(py_file.read_text(encoding="utf-8"), filename=str(py_file))
        except (SyntaxError, UnicodeDecodeError):
            continue
        blueprints: dict[str, str] = {}
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign) and isinstance(node.value, ast.Call):
                fn = node.value.func
                is_bp = (isinstance(fn, ast.Name) and fn.id == "Blueprint") or (isinstance(fn, ast.Attribute) and fn.attr == "Blueprint")
                if is_bp and node.value.args:
                    bp_name = _string(node.value.args[0])
                    if bp_name:
                        for target in node.targets:
                            if isinstance(target, ast.Name):
                                blueprints[target.id] = bp_name
        for node in ast.walk(tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            for deco in node.decorator_list:
                if not isinstance(deco, ast.Call) or not isinstance(deco.func, ast.Attribute):
                    continue
                if deco.func.attr not in {"route", "get", "post", "put", "patch", "delete"}:
                    continue
                owner = deco.func.value
                owner_name = owner.id if isinstance(owner, ast.Name) else None
                endpoint = node.name
                for kw in deco.keywords:
                    if kw.arg == "endpoint":
                        endpoint = _string(kw.value) or endpoint
                if owner_name == "app":
                    endpoints.add(endpoint)
                elif owner_name in blueprints:
                    endpoints.add(f"{blueprints[owner_name]}.{endpoint}")
    return endpoints


def main() -> int:
    endpoints = collect_endpoints()
    missing: dict[str, list[str]] = {}
    unreadable: list[str] = []
    templates = sorted((ROOT / "templates").rglob("*.html"))
    for template in templates:
        rel = template.relative_to(ROOT).as_posix()
        try:
            source = template.read_text(encoding="utf-8")
        except UnicodeDecodeError as exc:
            unreadable.append(f"{rel}: {exc}")
            continue
        for endpoint in URL_FOR_RE.findall(source):
            if endpoint not in endpoints:
                missing.setdefault(endpoint, []).append(rel)

    if unreadable:
        print("TEMPLATES COM CODIFICAÇÃO INVÁLIDA:")
        for item in unreadable:
            print(f" - {item}")
    if missing:
        print("ROTAS REFERENCIADAS MAS NÃO REGISTRADAS:")
        for endpoint, files in sorted(missing.items()):
            print(f" - {endpoint}: {', '.join(sorted(set(files)))}")
    if unreadable or missing:
        return 1
    print(f"Templates verificados: {len(templates)}.")
    print(f"Endpoints encontrados: {len(endpoints)}.")
    print("Nenhum url_for aponta para uma rota inexistente.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
