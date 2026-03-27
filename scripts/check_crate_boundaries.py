#!/usr/bin/env python3

import json
import subprocess
import sys


WORKSPACE_PACKAGES = {
    "bot",
    "builder",
    "detection",
    "domain",
    "reconciliation",
    "signerd",
    "signing",
    "state",
    "strategy",
    "submit",
    "telemetry",
}

ALLOWED_INTERNAL_DEPS = {
    "domain": set(),
    "telemetry": set(),
    "detection": {"domain"},
    "state": {"domain"},
    "strategy": {"domain", "state"},
    "builder": {"domain", "strategy"},
    "signing": {"builder", "domain"},
    "submit": {"domain", "signing"},
    "reconciliation": {"domain", "submit"},
    "signerd": {"signing"},
    "bot": {
        "builder",
        "detection",
        "domain",
        "reconciliation",
        "signerd",
        "signing",
        "state",
        "strategy",
        "submit",
        "telemetry",
    },
}


def main() -> int:
    output = subprocess.check_output(
        ["cargo", "metadata", "--format-version", "1", "--no-deps"],
        text=True,
    )
    metadata = json.loads(output)
    packages = {
        package["name"]: package
        for package in metadata["packages"]
        if package["name"] in WORKSPACE_PACKAGES
    }

    violations = []
    for package_name, package in sorted(packages.items()):
        internal_deps = {
            dependency["name"]
            for dependency in package["dependencies"]
            if dependency["name"] in WORKSPACE_PACKAGES and dependency["kind"] != "dev"
        }
        allowed = ALLOWED_INTERNAL_DEPS.get(package_name, set())
        unexpected = sorted(internal_deps - allowed)
        if unexpected:
            violations.append(
                f"{package_name} has forbidden internal deps: {', '.join(unexpected)}"
            )

    if violations:
        for violation in violations:
            print(f"boundary violation: {violation}", file=sys.stderr)
        return 1

    print("crate boundary check passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
